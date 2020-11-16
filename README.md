# atoolbox
## Simple functions for spark

### installation
```sh
$ pip install atoolbox
```

## Available functions:
- ### join_dataframes
- ### toJson
- ### add_numeric_fields


---

## Examples:
- ### join_dataframes
```python

joined_df = tbs.join_dataframes(df_list, on="id", how="left")

```

- ### toJson
```python

df = tbs.toJson(dfa) # a file is written by default at file:////tmp/out

```
- ### add_numeric_fields
```python

df = tbs.add_numeric_fields(dfc,"id")

```