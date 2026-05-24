import platform


def is_mac():
    return platform.system() == "Darwin"

def main():
    print(f"ADB_PORT={55557 if is_mac() else 55555}")

if __name__ == "__main__":
    main()