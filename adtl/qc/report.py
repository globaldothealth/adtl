"""
Quality Control module for ADTL, report submodule
"""

import json
import datetime
from string import Template
from pathlib import Path
from typing import List, Any, Dict
from functools import partial

import pandas as pd

from . import Rule

RULES_SUBFOLDER = "r"
DATASET_SUBFOLDER = "d"
TEMPLATES = Path(__file__).parent / "templates"
STYLE = TEMPLATES / "style.css"
INDEX = "index.html"


def render_result(result: Dict[str, Any], show_rule: bool = False) -> str:
    result["reason"] = result.get("reason", "")
    result["reason_str"] = (
        f"<span class=\"reason\">{result['reason']}</span>" if result["reason"] else ""
    )
    result["rule_str"] = (
        f"""<a href="../r/{result["rule"]}.html">{result["rule"]}</a>, """
        if show_rule
        else ""
    )
    if not result["success"]:
        if result["reason_str"] and result["mostly"] == 0:  # schema reasons
            tmpl = (
                "<li>{rows_fail} fail; {rule_str} <strong>{dataset}</strong>: {file}{reason_str}"
            ).format(**result)
        else:
            tmpl = "<li>{rows_fail} fail / {rows} total; {rule_str}{reason_str}<strong>{dataset}</strong>: {file}".format(
                **result
            )
    else:
        tmpl = (
            "<li>✔ {rule_str} {reason_str}<strong>{dataset}</strong>: {file}</li>"
        ).format(**result)

    if (
        result.get("fail_data")
        and not (fail_data := pd.DataFrame(json.loads(result["fail_data"]))).empty
    ):
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
    results: pd.DataFrame, rules: List[Rule], timestamp: datetime.datetime
) -> Dict[str, str]:
    def results_for_rule(rule_name: str) -> str:
        return "\n".join(
            map(
                render_result,
                results[results.rule == rule_name].to_dict(orient="records"),
            )
        )  # type: ignore

    out = {}
    for rule_name in results.rule.unique():
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
                timestamp=timestamp.strftime("%c"),
            )
        )
    return out


def render_results_by_dataset(
    results: pd.DataFrame, datasets: List[str], timestamp: datetime.datetime
) -> Dict[str, str]:
    out = {}

    for dataset in datasets:
        result_data = "\n".join(
            map(
                partial(render_result, show_rule=True),
                results[results.dataset == dataset].to_dict(orient="records"),
            )
        )  # type: ignore
        out[dataset] = Template((TEMPLATES / "dataset.html").read_text()).substitute(
            dataset=dataset, results=result_data, timestamp=timestamp.strftime("%c")
        )
    return out


def render_index(
    rules: List[Rule], datasets: List[str], timestamp: datetime.datetime
) -> str:
    dataset_index = "\n".join(
        f"""<li><a href="d/{dataset}.html">{dataset}</a></li>""" for dataset in datasets
    )
    rule_index = "\n".join(
        f"""<li><a href="r/{r["name"]}.html">{r["description"]}</a></li>"""
        for r in rules
    )
    return Template((TEMPLATES / "index.html").read_text()).substitute(
        dict(
            dataset_index=dataset_index,
            rule_index=rule_index,
            timestamp=timestamp.strftime("%c"),
        )
    )


def make_report(
    results: pd.DataFrame, rules: List[Rule], output_folder: Path = Path("qc_report")
):
    "Makes report from results database"

    ts = datetime.datetime.utcnow()
    output_folder.mkdir(exist_ok=True)
    (output_folder / "r").mkdir(exist_ok=True)
    (output_folder / "d").mkdir(exist_ok=True)

    datasets = list(results.dataset.unique())
    (output_folder / "style.css").write_text(STYLE.read_text())
    (output_folder / INDEX).write_text(render_index(rules, datasets, ts))

    results_by_rule = render_results_by_rule(results, rules, ts)
    results_by_dataset = render_results_by_dataset(results, datasets, ts)
    for rule in results_by_rule:
        (output_folder / "r" / (rule + ".html")).write_text(results_by_rule[rule])
        print(f"wrote r/{rule}.html")
    for dataset in datasets:
        (output_folder / "d" / (dataset + ".html")).write_text(
            results_by_dataset[dataset]
        )
        print(f"wrote d/{dataset}.html")
