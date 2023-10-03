---
title: Quality control
---

# Quality control

ADTL offers a quality control (QC) tool called adtl-qc. This helps in assessing dataset quality beyond that offered by the adtl tool that include JSON schema validity, by allowing arbitrary Python rule files within the `qc` directory. This tool is initially intended to be used for small datasets that fit in memory and can be read into a Pandas dataframe. Support may be added later for reading databases.

Each rule takes a dataframe and returns a boolean Pandas series (or the underlying iterable or numpy array). The series should indicate True when the required assertion is valid and False when the required assertions are invalid. The ideal scenario is when assertion passes for all rows, and returns an array of True.

```python
@rule("Male patients are not pregnant", require_columns=["sex", "sex_at_birth", "pregnancy"], pattern="*-subject.csv")
def male_patients_not_pregnant(df: pd.DataFrame) -> pd.Series:
    return (df.sex == "male" | df.sex_at_birth == "male") & (df.pregnancy != True)
```

```python
subject_schema = schema("schemas/dev/subject.schema.json", pattern="*-subject.csv")

```
adtl-qc takes the rules (which can be hierarchically organised into submodules) and outputs a summary as a SQLite DB, which can be viewed by the frontend application `adtl-qc-viewer`.

```
dataset,rule,count_success,count_fail,ratio_success,test_run,failure_instances,success

test_run,test_date

rule,rule_description
```

```
Rules

pregnancy
   * Male patients are not pregnant (fails in 1/10 datasets)

   SELECT dataset from qcruns where rule = " and not success
   * Male patients do not have pregnancy related metadata

Datasets

    * isaric-ccpuk

    select * from qcruns where dataset = "isaric-ccpuk"

    * stopcovid-russia

```

