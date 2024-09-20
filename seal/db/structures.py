from typing import Any, Dict


class Structures:
    def __init__(self):
        self.structure_dict: Dict[str, Dict[str, Any]] = {}

    def register(self, data_source: str, database: str, table: str, structure: Any):
        if f'{data_source}.{database}' not in self.structure_dict:
            self.structure_dict[f'{data_source}.{database}'] = {}
        self.structure_dict[f'{data_source}.{database}'][table] = structure

    def get(self, data_source: str, database: str, table: str) -> Any:
        if f'{data_source}.{database}' not in self.structure_dict:
            return None
        if table not in self.structure_dict[f'{data_source}.{database}']:
            return None
        return self.structure_dict[f'{data_source}.{database}'][table]


structures = Structures()
