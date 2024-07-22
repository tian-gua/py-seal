class Condition:
    def __init__(self, field, value, operator='='):
        self.field = field
        self.value = value
        self.operator = operator

    def parse(self):
        return f'{self.field} {self.operator} ?', self.value


class ConditionTree:
    def __init__(self, logic='and'):
        self.conditions = []
        self.logic = logic

    def or_(self):
        self.logic = 'or'
        return self

    def add_condition(self, condition: Condition):
        self.conditions.append(condition)
        return self

    def add_tree(self, condition_tree):
        self.conditions.append(condition_tree)
        return self

    def parse(self):
        if len(self.conditions) == 0:
            return None
        args = []
        exps = []
        for condition in self.conditions:
            if isinstance(condition, ConditionTree):
                exp, arg = condition.parse()
                exps.append(f'({exp})')
                args.extend(arg)
            else:
                exp, arg = condition.parse()
                exps.append(exp)
                args.append(arg)
        return f' {self.logic} '.join(exps), tuple(args)
