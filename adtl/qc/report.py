"""
Quality Control module for ADTL, report submodule
"""

import json
import sqlite3
from string import Template
from pathlib import Path
from typing import List, Any, Dict
from functools import partial

import pandas as pd

from . import get_result_from_insertion, WorkUnitResult, Rule

RULES_SUBFOLDER = "r"
DATASET_SUBFOLDER = "d"
TEMPLATES = Path(__file__).parent / "templates"
STYLE = TEMPLATES / "style.css"
INDEX = "index.html"


def render_result(result: Dict[str, Any], show_rule: bool = False) -> str:
    result = get_result_from_insertion(result)  # type: ignore
    result["reason"] = result.get("reason", "")
    result["reason_str"] = f" ({result['reason']}) " if result["reason"] else ""
    result["rule_str"] = (
        f"""<a href="../r/{result["rule"]}.html">{result["rule"]}</a>, """
        if show_rule
        else ""
    )
    tmpl = (
        "<li><tt>[{rows_fail} / {rows}]</tt> {rule_str}{reason_str}{dataset} / {file}".format(
            **result
        )
        if result["success"] != 1
        else "<li>✔ {rule_str}{reason_str}{dataset} / {file}</li>".format(**result)
    )
    if result.get("fail_data"):
        fail_data = pd.DataFrame(json.loads(result["fail_data"]))
        tmpl += """
  <details>
    <summary>Failed rows</summary>
    <pre>{log}</p>
  </details></li>""".format(
            log=str(fail_data)
        )
    else:
        tmpl += "</li>"
    return tmpl


def render_results_by_rule(
    results: List[WorkUnitResult], rules: List[Rule]
) -> Dict[str, str]:
    def results_for_rule(rule_name: str) -> str:
        return "\n".join(render_result(r) for r in results if r["rule"] == rule_name)  # type: ignore

    out = {}
    for rule_name in set(r["rule"] for r in results):
        rule = [r for r in rules if r["name"] == rule_name][0]
        out[rule_name] = Template((TEMPLATES / "rule.html").read_text()).substitute(
            dict(
                name=rule["name"],
                description=rule["description"],
                long_description='<p class="long_description">\n'
                + rule["long_description"]
                + "</p>"
                if rule["long_description"]
                else "",
                results=results_for_rule(rule_name),
            )
        )
    return out


def render_results_by_dataset(
    results: List[WorkUnitResult], datasets: List[str]
) -> Dict[str, str]:
    def filter_dataset(dataset: str) -> List[WorkUnitResult]:
        return [r for r in results if r["dataset"] == dataset]

    out = {}

    for dataset in datasets:
        result_data = "\n".join(
            map(partial(render_result, show_rule=True), filter_dataset(dataset))
        )
        out[dataset] = Template((TEMPLATES / "dataset.html").read_text()).substitute(
            dataset=dataset, results=result_data
        )
    return out


def render_index(rules: List[Dict[str, Any]], datasets: List[str]) -> str:
    dataset_index = "\n".join(
        f"""<li><a href="d/{dataset}.html">{dataset}</a></li>""" for dataset in datasets
    )
    rule_index = "\n".join(
        f"""<li><a href="r/{r["name"]}.html">{r["description"]}</a></li>"""
        for r in rules
    )
    return Template((TEMPLATES / "index.html").read_text()).substitute(
        dict(dataset_index=dataset_index, rule_index=rule_index)
    )


def read_sql(
    conn: sqlite3.Connection, sql: str, columns: List[str]
) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    res = cur.execute(sql)
    return [dict(zip(columns, r)) for r in res.fetchall()]


def make_report(store_database: str, output_folder: Path = Path("qc_report")):
    "Makes report from results database"

    output_folder.mkdir(exist_ok=True)
    (output_folder / "r").mkdir(exist_ok=True)
    (output_folder / "d").mkdir(exist_ok=True)

    conn = sqlite3.connect(store_database)
    datasets = read_sql(conn, "SELECT DISTINCT dataset FROM results", ["dataset"])
    datasets = [n["dataset"] if n["dataset"] else "_unlabelled" for n in datasets]
    rules = read_sql(
        conn,
        "SELECT name, description, long_description FROM rules",
        ["name", "description", "long_description"],
    )
    (output_folder / "style.css").write_text(STYLE.read_text())
    (output_folder / INDEX).write_text(render_index(rules, datasets))
    results = read_sql(
        conn,
        "SELECT * from results",
        [
            "rule",
            "dataset",
            "file",
            "rows_success",
            "rows_fail",
            "rows",
            "ratio_success",
            "rows_fail_idx",
            "success",
            "mostly",
            "reason",
            "fail_data",
        ],
    )
    results_by_rule = render_results_by_rule(results, rules)
    results_by_dataset = render_results_by_dataset(results, datasets)
    for rule in results_by_rule:
        (output_folder / "r" / (rule + ".html")).write_text(results_by_rule[rule])
        print(f"wrote r/{rule}.html")
    for dataset in datasets:
        (output_folder / "d" / (dataset + ".html")).write_text(
            results_by_dataset[dataset]
        )
        print(f"wrote d/{dataset}.html")


if __name__ == "__main__":
    make_report("adtl-qc.db")
