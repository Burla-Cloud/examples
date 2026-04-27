from burla import remote_parallel_map


def cube_number(x: int) -> dict:
    return {"input": x, "cube": x * x * x}


def main() -> None:
    inputs = list(range(20))
    print("Local validation:", [cube_number(i) for i in inputs[:3]])

    results = remote_parallel_map(cube_number, inputs, grow=True)
    print("REMOTE_OK")
    print("result_count=", len(results))
    print("sample=", results[:5])


if __name__ == "__main__":
    main()
