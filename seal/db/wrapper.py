from .condition import ConditionTree, Condition
from typing import Self


class Wrapper:
    def __init__(self,
                 tenant_field=None,
                 tenant_value=None,
                 logic_delete_field=None,
                 logic_delete_true=None,
                 logic_delete_false=None):
        self.condition_tree = ConditionTree()
        self.tenant_field = tenant_field
        self.tenant_value = tenant_value
        self.logic_delete_field = logic_delete_field
        self.logic_delete_true = logic_delete_true
        self.logic_delete_false = logic_delete_false

    def eq(self, field, value) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, value))
        return self

    def ne(self, field, value) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, value, '!='))
        return self

    def gt(self, field, value) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, value, '>'))
        return self

    def ge(self, field, value) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, value, '>='))
        return self

    def lt(self, field, value) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, value, '<'))
        return self

    def le(self, field, value) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, value, '<='))
        return self

    def in_(self, field, value) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, value, 'in'))
        return self

    def l_like(self, field, value) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, f'%{value}', 'like'))
        return self

    def r_like(self, field, value) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, f'{value}%', 'like'))
        return self

    def like(self, field, value) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, f'%{value}%', 'like'))
        return self

    def or_(self, wrapper: Self) -> 'Wrapper':
        wrapper.condition_tree.or_()
        self.condition_tree.add_tree(wrapper.condition_tree)
        return self

    def handle_public_fields(self, **options):
        if 'logical_delete' in options:
            self.eq(options['logical_delete'], 0)
            return
        if self.tenant_field is not None:
            if self.tenant_value is None:
                raise ValueError('tenant value not set')
            self.eq(self.tenant_field, self.tenant_value)

        if self.logic_delete_field is not None:
            if self.logic_delete_true is None or self.logic_delete_false is None:
                raise ValueError('logic delete field and value not set')
            self.eq(self.logic_delete_field, self.logic_delete_false)
