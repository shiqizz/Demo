# -*- coding: utf-8
from rest_framework import serializers


def remove_exponent(num):
    return num.to_integral() if num == num.to_integral() else num.normalize()


class SerializerMethodTupleField(serializers.Field):
    """
    For example:

    class ExampleSerializer(self):
        extra_info = SerializerMethodTupleField(tuple_num=1)

        def get_extra_info(self, obj):
            return a,b  # Calculate some data to return.
    """
    def __init__(self, method_name=None, tuple_num=0, **kwargs):
        self.method_name = method_name
        self.tuple_num = tuple_num
        kwargs['source'] = '*'
        kwargs['read_only'] = True
        super(SerializerMethodTupleField, self).__init__(**kwargs)

    def bind(self, field_name, parent):
        default_method_name = 'get_{field_name}'.format(field_name=field_name)
        assert self.method_name != default_method_name, (
            "It is redundant to specify `%s` on SerializerMethodField '%s' in "
            "serializer '%s', because it is the same as the default method name. "
            "Remove the `method_name` argument." %
            (self.method_name, field_name, parent.__class__.__name__)
        )
        if self.method_name is None:
            self.method_name = default_method_name

        super(SerializerMethodTupleField, self).bind(field_name, parent)

    def to_representation(self, value):
        method = getattr(self.parent, self.method_name)
        res = method(value)
        if isinstance(res, tuple):
            return res[self.tuple_num]


class NewDecimalField(serializers.DecimalField):
    def quantize(self, value):
        num = super(NewDecimalField, self).quantize(value)
        return remove_exponent(num)
