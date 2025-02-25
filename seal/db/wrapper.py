from .condition import ConditionTree, Condition
from ..enum.operator import Operator
from ..types import Column


class Wrapper:
    def __init__(self,
                 tenant_field: Column | None = None,
                 tenant_value: any = None,
                 logical_deleted_field: Column | None = None,
                 logical_deleted_value_true: any = None,
                 logical_deleted_value_false: any = None):
        self.condition_tree = ConditionTree()
        self.tenant_field: Column | None = tenant_field
        self.tenant_value: any = tenant_value
        self.logical_deleted_field: Column | None = logical_deleted_field
        self.logical_deleted_value_true: any = logical_deleted_value_true
        self.logical_deleted_value_false: any = logical_deleted_value_false

    def eq(self, field: Column, value: any) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, value))
        return self

    def ne(self, field: Column, value: any) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, value, Operator.NE))
        return self

    def gt(self, field: Column, value: any) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, value, Operator.GT))
        return self

    def ge(self, field: Column, value: any) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, value, Operator.GE))
        return self

    def lt(self, field: Column, value: any) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, value, Operator.LT))
        return self

    def le(self, field: Column, value: any) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, value, Operator.LE))
        return self

    def in_(self, field: Column, value: any) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, value, Operator.IN))
        return self

    def l_like(self, field: Column, value: any) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, f'%{value}', Operator.LIKE))
        return self

    def r_like(self, field: Column, value: any) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, f'{value}%', Operator.LIKE))
        return self

    def like(self, field: Column, value: any) -> 'Wrapper':
        self.condition_tree.add_condition(Condition(field, f'%{value}%', Operator.LIKE))
        return self

    def or_(self, wrapper: 'Wrapper') -> 'Wrapper':
        wrapper.condition_tree.or_()
        self.condition_tree.add_tree(wrapper.condition_tree)
        return self

    def handle_public_fields(self, **options):
        if 'logical_deleted' in options:
            self.eq(options['logical_deleted'], 0)
            return
        if self.tenant_field is not None:
            if self.tenant_value is None:
                raise ValueError('tenant value not set')
            self.eq(self.tenant_field, self.tenant_value)

        if self.logical_deleted_field is not None:
            if self.logical_deleted_value_true is None or self.logical_deleted_value_false is None:
                raise ValueError('logical deleted field and value not set')
            self.eq(self.logical_deleted_field, self.logical_deleted_value_false)
