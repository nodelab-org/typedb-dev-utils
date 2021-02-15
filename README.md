# py-poetry-package-template

Template for a python package that bundles dependencies using poetry (https://python-poetry.org/)
In addition to the standard initial poetry project template, includes
* `/new_package/data` directory, which can be included in sdist and/or wheel using the `include` key in `pyproject.tml` and the last line of `Distribution / packaging` in `.gitignore`

References:
* [Can not exclude package data file only for the wheel #3380](https://github.com/python-poetry/poetry/issues/3380)
* [example pyproject.toml](https://github.com/zoj613/htnorm/blob/main/pyproject.toml)

## quick start

install poetry

```
pip install poetry
```

```
git clone https://github.com/JonThom/py-poetry-package-template.git
cd py-poetry-package-template
```

create a virtual environment

```
python3 -m venv .venv
```

from there on, follow the [poetry docs](https://python-poetry.org/docs/)

