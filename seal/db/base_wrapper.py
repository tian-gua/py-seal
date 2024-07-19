from .condition import ConditionTree, Condition
from .data_source import DataSource


class BaseWrapper:
    def __init__(self, table: str, data_source: DataSource, logical_delete=None):
        self.table = table
        self.condition_tree = ConditionTree()
        self.data_source = data_source
        self.logical_delete = logical_delete

    def eq(self, field, value):
        self.condition_tree.add_condition(Condition(field, value))
        return self

    def ne(self, field, value):
        self.condition_tree.add_condition(Condition(field, value, '!='))
        return self

    def gt(self, field, value):
        self.condition_tree.add_condition(Condition(field, value, '>'))
        return self

    def ge(self, field, value):
        self.condition_tree.add_condition(Condition(field, value, '>='))
        return self

    def lt(self, field, value):
        self.condition_tree.add_condition(Condition(field, value, '<'))
        return self

    def le(self, field, value):
        self.condition_tree.add_condition(Condition(field, value, '<='))
        return self

    def in_(self, field, value):
        self.condition_tree.add_condition(Condition(field, value, 'in'))
        return self

    def l_like(self, field, value):
        self.condition_tree.add_condition(Condition(field, f'%{value}', 'like'))
        return self

    def r_like(self, field, value):
        self.condition_tree.add_condition(Condition(field, f'{value}%', 'like'))
        return self

    def like(self, field, value):
        self.condition_tree.add_condition(Condition(field, f'%{value}%', 'like'))
        return self

    def or_(self, wrapper):
        self.condition_tree.add_tree(wrapper.condition_tree)
        return self

    def _handle_logical_delete(self, **options):
        if 'logical_delete' in options:
            self.eq(options['logical_delete'], 0)
            return

        if self.logical_delete is not None:
            self.eq(self.logical_delete, 0)
