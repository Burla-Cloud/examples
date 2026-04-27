from statistics import mean

from burla import remote_parallel_map


def feature_job(x: int) -> dict:
    """Compute a few simple derived features for a numeric input."""
    square = x * x
    cube = x * x * x
    parity = "even" if x % 2 == 0 else "odd"
    return {
        "input": x,
        "square": square,
        "cube": cube,
        "parity": parity,
        "score": (square + cube) % 97,
    }


def main() -> None:
    inputs = list(range(50))

    # Local validation first.
    local_results = [feature_job(i) for i in inputs]
    print("LOCAL_OK")
    print("local_count=", len(local_results))
    print("local_avg_score=", round(mean(r["score"] for r in local_results), 3))

    remote_results = remote_parallel_map(feature_job, inputs, grow=True)
    print("REMOTE_OK")
    print("remote_count=", len(remote_results))
    print("remote_sample=", remote_results[:5])


if __name__ == "__main__":
    main()
