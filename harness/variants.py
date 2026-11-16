"""Generate query variants from Jinja templates and a TOML spec (queries/variants.toml)."""

import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

SPEC = Path(__file__).parent.parent / "queries" / "variants.toml"


@dataclass
class Query:
    name: str  # name of the base query("q2") or a variant ("q2_divisor_1")
    sql: str  # the rendered template: sink table + INSERT + additional setup queries
    param: str | None = None  # the parameter this variant changes
    value: object = None  # the value of the changed parameter

    @property
    def is_base(self):
        return self.param is None


class VariantGenerator:
    def __init__(self, spec=SPEC):
        spec = Path(spec)
        env = Environment(
            loader=FileSystemLoader(spec.parent), undefined=StrictUndefined
        )
        self.templates = {}
        self.defaults = {}
        self.variants = {}
        for query, entry in tomllib.loads(spec.read_text()).items():
            self.templates[query] = env.get_template(entry.pop("template"))
            for param, values in entry.items():
                if not isinstance(values, dict) or "default" not in values:
                    raise ValueError(
                        f"{query}.{param}: expected {{ default = ..., variants = [...] }}"
                    )
            self.defaults[query] = {
                param: values["default"] for param, values in entry.items()
            }
            self.variants[query] = {
                param: values.get("variants", []) for param, values in entry.items()
            }
            if "name" in self.defaults[query]:
                raise ValueError(f"{query}: 'name' is reserved for the sink table name")
            self.render(query, query)

    def generate(self, query, param=None):
        """Return (base, variants) of a query: variants of every parameter, or only of `param`."""
        if query not in self.templates:
            raise ValueError(
                f"unknown query {query!r}, expected one of {list(self.templates)}"
            )
        if param is not None and param not in self.defaults[query]:
            raise ValueError(
                f"{query} has no parameter {param!r}, expected one of {list(self.defaults[query])}"
            )
        params = list(self.variants[query]) if param is None else [param]

        base = Query(query, self.render(query, query))
        variants = []
        for changed in params:
            for i, value in enumerate(self.variants[query][changed], start=1):
                variant = f"{query}_{changed}_{i}"
                variants.append(
                    Query(
                        variant,
                        self.render(query, variant, {changed: value}),
                        changed,
                        value,
                    )
                )
        return base, variants

    def render(self, query, name, changes=None):
        """The query's SQL with its defaults, overridden by `changes`, writing to a sink called `name`."""
        return (
            self.templates[query]
            .render(self.defaults[query] | (changes or {}), name=name)
            .strip()
        )


if __name__ == "__main__":
    base, variants = VariantGenerator().generate(*sys.argv[1:3])
    for query in [base, *variants]:
        label = "base" if query.is_base else f"{query.param} = {query.value!r}"
        print(f"-- {query.name} ({label})\n{query.sql};\n")
