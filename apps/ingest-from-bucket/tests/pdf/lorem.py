#!/usr/bin/python3

# pip install lorem-text

from lorem_text import lorem
from subprocess import run

if __name__ == '__main__':
    for i in range(0,10):
        print(i)
        with open(f"lorem_{i}.txt","w") as f:
            f.write(lorem.paragraph())

        run( f"ex -c ':hardcopy > lorem_{i}.ps | q' lorem_{i}.txt", shell=True)
        run( f"ps2pdf lorem_{i}.ps lorem_{i}.pdf", shell=True)
