---
title: Quality control
---

# Data Quality Control

ADTL offers a quality control (QC) tool called adtl-qc. This helps in assessing dataset quality beyond that offered by the adtl tool that include JSON schema validity, by allowing arbitrary Python rule files within the `qc` directory. This tool is initially intended to be used for small datasets that fit in memory and can be read into a Pandas dataframe. Support may be added later for reading databases.

Each rule takes a dataframe and returns a boolean Pandas series (or the underlying iterable or numpy array). The series should indicate True when the required assertion is valid and False when the required assertions are invalid. The ideal scenario is when assertion passes for all rows, and returns an array of True.

```python
from adtl.qc import rule, schema

@rule(require_columns=["sex", "sex_at_birth", "pregnancy"], pattern="*-subject.csv")
def male_patients_not_pregnant(df: pd.DataFrame) -> pd.Series:
    "Male patients are not pregnant"
    return (df.sex == "male" | df.sex_at_birth == "male") & (df.pregnancy != True)
```

```python
subject_schema = schema("schemas/dev/subject.schema.json", pattern="*-subject.csv")
```
adtl-qc takes the rules (which are organised into submodules) and outputs a summary as a SQLite DB.

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

## Running 

Once you have the rules setup under a `qc` folder in the root of your repository, you can run dqc as follows:

```shell
adtl-qc <dataroot>
```

where `<dataroot>` is the folder of data files that you want to do QC on. This will create
a qc.db in your root folder that is a SQLite database containing the outputs of QC and an HTML output in `qc_report` that you can open as follows `open qc_report/index.html`.