def append_to(file, content):
    with open(file, "a") as stream:
        stream.write(content)
