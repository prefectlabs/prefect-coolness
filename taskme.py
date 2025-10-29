import sys

from prefect import flow, task


@task
def double(i: int):
    return i * 2


@flow
def double_em(count: int) -> int:
    total = 0
    for i in range(count):
        total += double(i)
    return total


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    print(double_em(count))
