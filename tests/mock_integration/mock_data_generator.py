"""Generic mock data generator for Singer tap integration tests.

Reads JSON schema files and generates mock API response data with
deterministic, type-conformant values.
"""
import json
import os
from datetime import datetime, timedelta


class MockDataGenerator:
    """Generates mock records from JSON schema files.

    Usage::

        gen = MockDataGenerator('/path/to/tap_pardot/schemas')
        record = gen.generate_record('prospects', seed=0)
        records = gen.generate_records('prospects', count=3)
    """

    BASE_DATE = datetime(2024, 6, 15, 10, 0, 0)

    def __init__(self, schemas_dir):
        self.schemas_dir = schemas_dir
        self._schema_cache = {}

    def load_schema(self, stream_name):
        """Load and cache a JSON schema file for *stream_name*."""
        if stream_name not in self._schema_cache:
            path = os.path.join(self.schemas_dir, f'{stream_name}.json')
            with open(path) as f:
                self._schema_cache[stream_name] = json.load(f)
        return self._schema_cache[stream_name]

    @staticmethod
    def resolve_type(type_spec):
        """Return the first non-null type from a JSON-Schema *type* field."""
        if isinstance(type_spec, list):
            for t in type_spec:
                if t != 'null':
                    return t
            return 'string'
        return type_spec

    def generate_value(self, field_name, field_schema, seed=0):
        """Return a deterministic value that conforms to *field_schema*."""
        type_str = self.resolve_type(field_schema.get('type', 'string'))
        fmt = field_schema.get('format')

        if fmt == 'date-time':
            dt = self.BASE_DATE + timedelta(days=seed)
            return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        if type_str == 'string':
            return f"mock-{field_name.lower()}-{seed}"
        if type_str == 'number':
            return round(42.5 + seed * 1.1, 2)
        if type_str == 'integer':
            return 100 + seed
        if type_str == 'boolean':
            return seed % 2 == 0
        if type_str == 'array':
            items_schema = field_schema.get('items', {})
            if items_schema.get('properties'):
                return [self._generate_object(items_schema, seed)]
            return []
        if type_str == 'object':
            props = field_schema.get('properties', {})
            if props:
                return self._generate_object(field_schema, seed)
            return {}
        return f"mock-{seed}"

    def _generate_object(self, schema, seed):
        """Generate a nested object from schema properties."""
        obj = {}
        for k, v in schema.get('properties', {}).items():
            obj[k] = self.generate_value(k, v, seed)
        return obj

    def generate_record(self, stream_name, seed=0, overrides=None,
                        exclude_fields=None):
        """Generate one mock record for *stream_name* from its schema."""
        schema = self.load_schema(stream_name)
        record = {}
        exclude = exclude_fields or set()
        for field_name, field_schema in schema.get('properties', {}).items():
            if field_name in exclude:
                continue
            record[field_name] = self.generate_value(field_name, field_schema, seed)
        if overrides:
            record.update(overrides)
        return record

    def generate_records(self, stream_name, count=1, base_seed=0,
                         overrides=None, exclude_fields=None):
        """Generate *count* mock records with incrementing seeds."""
        return [
            self.generate_record(stream_name, seed=base_seed + i,
                                 overrides=overrides,
                                 exclude_fields=exclude_fields)
            for i in range(count)
        ]
