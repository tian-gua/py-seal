from .condition import ConditionTree, Condition


class Wrapper:
    def __init__(self,
                 tenant_field=None,
                 tenant_value=None,
                 logical_deleted_field=None,
                 logical_deleted_value_true=None,
                 logical_deleted_value_false=None):
        self.condition_tree = ConditionTree()
        self.tenant_field = tenant_field
        self.tenant_value = tenant_value
        self.logical_deleted_field = logical_deleted_field
        self.logical_deleted_value_true = logical_deleted_value_true
        self.logical_deleted_value_false = logical_deleted_value_false

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
