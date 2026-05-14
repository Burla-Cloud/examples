"""Download streaming + parse_hospital_mrf worker for Burla."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests

try:
    from curl_cffi import requests as _cffi_requests  # noqa: F401
    _HAS_CURL_CFFI = True
except Exception:
    _HAS_CURL_CFFI = False

from codes import CODES
from parsers_inline import pick_parser

REPO_ROOT = Path(__file__).resolve().parent


def shared_hpt_root() -> Path:
    env = os.environ.get("HPT_SHARED_ROOT")
    if env:
        return Path(env)
    w = Path("/workspace/shared/hpt")
    if Path("/workspace").exists() and (Path("/workspace") / "shared").exists():
        return w
    return REPO_ROOT / "scratch" / "hpt"


def build_target_codes() -> set[tuple[str, str]]:
    t: set[tuple[str, str]] = set()
    for c in CODES:
        sysu = c["code_system"].upper().replace(" ", "").replace("_", "")
        if sysu in ("MSDRG",):
            sysu = "MS-DRG"
        cod = c["code"].replace(".", "").replace(" ", "").replace("-", "")
        t.add((sysu, cod))
        t.add(("CPT", cod))
        t.add(("HCPCS", cod))
        if sysu == "NDC":
            t.add(("NDC", cod))
    return t


TARGET_CODES = build_target_codes()


def download_streaming(
    url: str,
    dest_dir: Path,
    timeout: int = 600,
    max_attempts: int = 3,
) -> Path:
    """Stream a URL to disk, with simple retries on incomplete reads / timeouts.

    Real-world MRFs from large health systems are 1-3 GB and many CDNs occasionally
    hang up mid-transfer. We retry with HTTP Range when possible so we do not have
    to redownload from byte 0.
    """
    import time

    dest_dir.mkdir(parents=True, exist_ok=True)
    tail = url.rsplit("/", 1)[-1].split("?")[0] or "mrf.dat"
    if len(tail) > 180:
        tail = hashlib.sha256(url.encode()).hexdigest()[:16] + ".dat"
    out = dest_dir / tail
    if out.exists() and out.stat().st_size > 0:
        return out

    from urllib.parse import urlparse

    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme else ""
    referer = f"{origin}/" if origin else ""
    headers_base = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "application/json;q=0.9,application/octet-stream;q=0.8,*/*;q=0.7"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Ch-Ua": '"Chromium";v="126", "Google Chrome";v="126", "Not.A/Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
    }
    if referer:
        headers_base["Referer"] = referer

    max_mb = os.environ.get("HPT_MAX_DOWNLOAD_MB")
    limit_bytes: int | None = None
    if max_mb:
        try:
            limit_bytes = int(float(max_mb) * 1024 * 1024)
        except ValueError:
            limit_bytes = None
    if limit_bytes:
        try:
            head = requests.head(
                url, headers=headers_base, allow_redirects=True, timeout=15
            )
            cl = head.headers.get("Content-Length")
            if cl and cl.isdigit() and int(cl) > limit_bytes:
                raise RuntimeError(
                    f"Skipping oversized MRF ({int(cl) // (1024 * 1024)}MB > {max_mb}MB cap)"
                )
        except RuntimeError:
            raise
        except Exception:
            pass

    last_err: Exception | None = None
    bytes_written = 0

    for attempt in range(max_attempts):
        try:
            req_headers = dict(headers_base)
            if bytes_written > 0:
                req_headers["Range"] = f"bytes={bytes_written}-"
            mode = "ab" if bytes_written > 0 else "wb"
            with requests.get(
                url,
                stream=True,
                timeout=timeout,
                headers=req_headers,
                allow_redirects=True,
            ) as r:
                if bytes_written > 0 and r.status_code == 200:
                    out.unlink(missing_ok=True)
                    bytes_written = 0
                    mode = "wb"
                r.raise_for_status()
                with open(out, mode) as f:
                    for chunk in r.iter_content(1 << 20):
                        if chunk:
                            f.write(chunk)
                            bytes_written += len(chunk)
                            if limit_bytes is not None and bytes_written > limit_bytes:
                                try:
                                    f.close()
                                except Exception:
                                    pass
                                out.unlink(missing_ok=True)
                                raise RuntimeError(
                                    f"Skipping oversized MRF "
                                    f"({bytes_written // (1024 * 1024)}MB > {max_mb}MB cap, "
                                    f"server omitted Content-Length)"
                                )
            return out
        except requests.HTTPError as e:
            # Akamai-style bot walls (Mount Sinai, Tufts) reject anything that does
            # not match Chrome's TLS JA3 fingerprint. Fall back to curl_cffi which
            # impersonates Chrome at the TLS layer, then continue with the normal
            # streaming download.
            if e.response is not None and e.response.status_code in (403, 401):
                try:
                    return _download_with_curl_cffi(
                        url,
                        out,
                        timeout=timeout,
                        limit_bytes=limit_bytes,
                        max_mb=max_mb,
                    )
                except Exception as cffi_err:
                    last_err = cffi_err
            else:
                last_err = e
            try:
                bytes_written = out.stat().st_size if out.exists() else 0
            except OSError:
                bytes_written = 0
            if attempt < max_attempts - 1:
                time.sleep(2 + attempt * 2)
                continue
            raise last_err
        except Exception as e:
            last_err = e
            try:
                bytes_written = out.stat().st_size if out.exists() else 0
            except OSError:
                bytes_written = 0
            if attempt < max_attempts - 1:
                time.sleep(2 + attempt * 2)
                continue
            raise
    if last_err is not None:
        raise last_err
    return out


def _download_with_curl_cffi(
    url: str,
    out: Path,
    timeout: int = 600,
    limit_bytes: int | None = None,
    max_mb: str | None = None,
) -> Path:
    """Stream-download via curl_cffi, impersonating Chrome's TLS handshake.

    Some hospital MRF hosts (Akamai-fronted: mountsinai.org, tuftsmedicine.org,
    selectmedical.com, etc.) reject anything without a Chrome-shaped TLS JA3
    fingerprint. Standard ``requests`` always 403s. ``curl_cffi`` provides a
    libcurl-impersonate build that does match Chrome's TLS fingerprint."""
    if not _HAS_CURL_CFFI:
        raise RuntimeError("curl_cffi not installed; cannot bypass Akamai 403")

    from curl_cffi import requests as cffi_requests

    bytes_written = 0
    out.unlink(missing_ok=True)
    r = cffi_requests.get(url, impersonate="chrome", stream=True, timeout=timeout)
    try:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                if not chunk:
                    continue
                f.write(chunk)
                bytes_written += len(chunk)
                if limit_bytes is not None and bytes_written > limit_bytes:
                    out.unlink(missing_ok=True)
                    raise RuntimeError(
                        f"Skipping oversized MRF "
                        f"({bytes_written // (1024 * 1024)}MB > {max_mb}MB cap, "
                        f"curl_cffi path)"
                    )
    finally:
        try:
            r.close()
        except Exception:
            pass
    return out


def normalize_row(row: dict, hospital: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        **row,
        "hospital_id": hospital.get("hospital_id"),
        "hospital_name": hospital.get("name"),
        "state": hospital.get("state"),
        "city": hospital.get("city"),
        "ccn": hospital.get("ccn"),
        "latitude": hospital.get("latitude"),
        "longitude": hospital.get("longitude"),
        "mrf_url": hospital.get("mrf_url"),
        "collected_at": now,
    }


def _cleanup_raw(raw_dir: Path) -> None:
    """Best-effort: remove the downloaded raw bytes after we've parsed them. Local disk is precious."""
    try:
        if raw_dir.is_dir():
            for p in raw_dir.iterdir():
                try:
                    if p.is_file():
                        p.unlink()
                    elif p.is_dir():
                        for q in p.iterdir():
                            try:
                                q.unlink()
                            except OSError:
                                pass
                        p.rmdir()
                except OSError:
                    pass
            try:
                raw_dir.rmdir()
            except OSError:
                pass
    except Exception:
        pass


FULL_CHARGEMASTER_ROW_CAP: int | None = None


def parse_hospital_mrf_full_chargemaster(hospital: dict) -> dict:
    """Burla worker: download one MRF, extract EVERY priced row (no code
    filter), and write it to a separate jsonl. When FULL_CHARGEMASTER_ROW_CAP
    is None, NO cap is applied and every priced row in the MRF is emitted.
    Otherwise rows are truncated to the cap.

    This is the input to the per-hospital "Full chargemaster" search view
    on hospital profile pages. The curated 360-code analysis runs from the
    separate observations/*.jsonl files written by parse_hospital_mrf().
    """
    hospital_id = hospital["hospital_id"]
    root = shared_hpt_root()
    raw_dir = root / "raw" / hospital_id
    chm_dir = root / "chargemaster_full"
    chm_dir.mkdir(parents=True, exist_ok=True)
    keep_raw = os.environ.get("HPT_KEEP_RAW", "0").lower() in ("1", "true", "yes")
    out_path = chm_dir / f"{hospital_id}.jsonl"
    fail_path = chm_dir / f"{hospital_id}.fail"

    if os.environ.get("HPT_SKIP_EXISTING", "1").lower() in ("1", "true", "yes"):
        if out_path.is_file() and out_path.stat().st_size > 0:
            try:
                with open(out_path, "r", encoding="utf-8") as f:
                    n = sum(1 for line in f if line.strip())
            except OSError:
                n = 0
            return {
                "hospital_id": hospital_id,
                "rows": n,
                "out": str(out_path),
                "error": None,
                "cached": True,
            }
        if fail_path.is_file() and os.environ.get("HPT_RETRY_FAILED", "0").lower() not in ("1", "true", "yes"):
            try:
                err = fail_path.read_text(encoding="utf-8").strip()[:160]
            except OSError:
                err = "previously failed"
            return {
                "hospital_id": hospital_id,
                "rows": 0,
                "error": err,
                "out": None,
                "cached": True,
            }

    try:
        raw_path = download_streaming(hospital["mrf_url"], raw_dir)
        sz_mb = raw_path.stat().st_size / 1e6
        parser = pick_parser(raw_path)
        truncated = False
        cap = FULL_CHARGEMASTER_ROW_CAP
        n_rows = 0
        tmp_path = out_path.with_suffix(".jsonl.tmp")
        with open(tmp_path, "w", encoding="utf-8") as fout:
            for r in parser.iter_priced_items(raw_path, None):
                fout.write(json.dumps(normalize_row(r, hospital)))
                fout.write("\n")
                n_rows += 1
                if cap is not None and n_rows >= cap:
                    truncated = True
                    break
        if n_rows == 0:
            tmp_path.write_text("\n", encoding="utf-8")
        tmp_path.replace(out_path)
        print(
            f"{hospital_id}: chargemaster {n_rows} rows"
            f"{' (truncated)' if truncated else ''} "
            f"from {raw_path.suffix} ({sz_mb:.1f}MB)"
        )
        if not keep_raw:
            _cleanup_raw(raw_dir)
        return {
            "hospital_id": hospital_id,
            "rows": n_rows,
            "truncated": truncated,
            "raw_size_mb": round(sz_mb, 1),
            "out": str(out_path),
            "error": None,
        }
    except Exception as e:
        if not keep_raw:
            _cleanup_raw(raw_dir)
        try:
            fail_path.write_text(f"{type(e).__name__}: {e}", encoding="utf-8")
        except OSError:
            pass
        print(f"{hospital_id}: chargemaster ERROR {e}")
        return {"hospital_id": hospital_id, "rows": 0, "error": str(e), "out": None}


def parse_hospital_mrf(hospital: dict) -> dict:
    """Burla worker: download one MRF, extract target code rows, write jsonl."""
    hospital_id = hospital["hospital_id"]
    root = shared_hpt_root()
    raw_dir = root / "raw" / hospital_id
    obs_dir = root / "observations"
    obs_dir.mkdir(parents=True, exist_ok=True)
    keep_raw = os.environ.get("HPT_KEEP_RAW", "0").lower() in ("1", "true", "yes")
    out_path = obs_dir / f"{hospital_id}.jsonl"
    fail_path = obs_dir / f"{hospital_id}.fail"

    if os.environ.get("HPT_SKIP_EXISTING", "1").lower() in ("1", "true", "yes"):
        if out_path.is_file() and out_path.stat().st_size > 0:
            try:
                with open(out_path, "r", encoding="utf-8") as f:
                    n = sum(1 for line in f if line.strip())
            except OSError:
                n = 0
            return {
                "hospital_id": hospital_id,
                "rows": n,
                "rows_parsed": n,
                "raw_size_mb": 0.0,
                "out": str(out_path),
                "error": None,
                "cached": True,
            }
        if fail_path.is_file() and os.environ.get("HPT_RETRY_FAILED", "0").lower() not in ("1", "true", "yes"):
            try:
                err = fail_path.read_text(encoding="utf-8").strip()[:160]
            except OSError:
                err = "previously failed"
            return {
                "hospital_id": hospital_id,
                "rows": 0,
                "error": err,
                "out": None,
                "cached": True,
            }

    try:
        raw_path = download_streaming(hospital["mrf_url"], raw_dir)
        sz_mb = raw_path.stat().st_size / 1e6
        parser = pick_parser(raw_path)
        parsed_rows = list(parser.iter_priced_items(raw_path, TARGET_CODES))
        rows = [normalize_row(r, hospital) for r in parsed_rows]
        if rows:
            out_path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
        else:
            out_path.write_text("\n", encoding="utf-8")
        print(
            f"{hospital_id}: parsed {len(parsed_rows)} rows from "
            f"{raw_path.suffix} ({sz_mb:.1f}MB)"
        )
        if not keep_raw:
            _cleanup_raw(raw_dir)
        return {
            "hospital_id": hospital_id,
            "rows": len(rows),
            "rows_parsed": len(parsed_rows),
            "raw_size_mb": round(sz_mb, 1),
            "out": str(out_path),
            "error": None,
        }
    except Exception as e:
        if not keep_raw:
            _cleanup_raw(raw_dir)
        try:
            fail_path.write_text(f"{type(e).__name__}: {e}", encoding="utf-8")
        except OSError:
            pass
        print(f"{hospital_id}: ERROR {e}")
        return {"hospital_id": hospital_id, "rows": 0, "error": str(e), "out": None}
