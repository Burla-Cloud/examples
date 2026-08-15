# Estimate a portfolio's hurricane tail risk

Insurers simulate many possible years to estimate losses that are too rare to observe directly. This example runs one million synthetic hurricane years and reports:

- Expected annual insured loss.
- The 1% aggregate exceedance probability (AEP) loss, the annual total exceeded in 1% of simulated years.
- The 0.4% AEP loss, the annual total exceeded in 0.4% of simulated years.

Each of 500 Burla tasks simulates 2,000 years against a $335B synthetic portfolio spread across 12,000 exposure cells. It returns only the nonempty parts of its loss histogram, so the client combines all one million years without downloading every simulated loss.

The hazard and damage assumptions are intentionally simple and cover wind only. This demonstrates the shape of a catastrophe-risk workload, not a model suitable for underwriting.

## Run it

Complete [Getting Started](https://docs.burla.dev/docs/get-started), then:

```bash
pip install -r requirements.txt
python main.py
```

```text
simulated years: 1,000,000
expected annual loss: $1.2B
1% AEP loss (1-in-100): about $19.6B
0.4% AEP loss (1-in-250): about $29.7B
elapsed: ...
```

The loss estimates are reproducible. Elapsed time depends on the machines in your Burla cluster.

