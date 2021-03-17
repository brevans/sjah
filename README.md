# Slurm Job Arrays Helper (`sjah`)

## Description

"ess-jah" or "shah"

## Quickstart

```
# create a venv or conda env with python>=3.6
python3 -m pip install sjah
rm -f jobfile.txt && for i in seq 5; do echo "sleep $((1 + $RANDOM % 10))" >> jobfile.txt; done;
sjah batch --submit jobfile.txt
```

## Usage

TODO

## Development

TODO
```
# create and activate a venv or conda env with python>=3.6
python3 -m pip install black flake8 flake8-import-order flake8-black pre-commit
python3 setup.py develop
```
