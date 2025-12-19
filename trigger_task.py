import sys
from tasks import main as create_records_task


def main():
    result = create_records_task.delay()
    print(f"Task submitted with ID: {result.id}")
    print("Waiting for task to complete...")
    sys.exit(1)


if __name__ == "__main__":
    main()
