"""Generate query variants from Jinja templates and a TOML spec (queries/variants.toml)."""

import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

SPEC = Path(__file__).parent.parent / "queries" / "variants.toml"


@dataclass
class Query:
    name: str                 # name of the base query("q2") or a variant ("q2_divisor_1")
    sql: str                  # the rendered sql template
    param: str | None = None  # the parameter this variant changes
    value: object = None      # the value of the changed parameter

    @property
    def is_base(self):
        return self.param is None


class VariantGenerator:
    def __init__(self, spec=SPEC):
        spec = Path(spec)
        env = Environment(loader=FileSystemLoader(spec.parent), undefined=StrictUndefined)
        self.templates = {}
        self.defaults = {}
        self.variants = {}
        for query, entry in tomllib.loads(spec.read_text()).items():
            self.templates[query] = env.get_template(entry.pop("template"))
            for param, values in entry.items():
                if not isinstance(values, dict) or "default" not in values:
                    raise ValueError(f"{query}.{param}: expected {{ default = ..., variants = [...] }}")
            self.defaults[query] = {param: values["default"] for param, values in entry.items()}
            self.variants[query] = {param: values.get("variants", []) for param, values in entry.items()}
            self.render(query)

    def generate(self, query, param=None):
        """Return (base, variants) of a query: variants of every parameter, or only of `param`."""
        if query not in self.templates:
            raise ValueError(f"unknown query {query!r}, expected one of {list(self.templates)}")
        if param is not None and param not in self.defaults[query]:
            raise ValueError(f"{query} has no parameter {param!r}, expected one of {list(self.defaults[query])}")
        params = list(self.variants[query]) if param is None else [param]

        base = Query(query, self.render(query))
        variants = []
        for name in params:
            for i, value in enumerate(self.variants[query][name], start=1):
                variants.append(Query(f"{query}_{name}_{i}", self.render(query, {name: value}), name, value))
        return base, variants

    def render(self, query, changes=None):
        """The query's SQL with its defaults, overridden by `changes`."""
        return self.templates[query].render(self.defaults[query] | (changes or {})).strip()


if __name__ == "__main__":
    base, variants = VariantGenerator().generate(*sys.argv[1:3])
    for query in [base, *variants]:
        label = "base" if query.is_base else f"{query.param} = {query.value!r}"
        print(f"-- {query.name} ({label})\n{query.sql};\n")
