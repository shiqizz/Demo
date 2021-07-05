# -*- coding: utf-8
from rest_framework import serializers
from .models import DemoModel


class DemoSerializer(serializers.ModelSerializer):
    class Meta:
        model = DemoModel  # 定义关联的 Model
        fields = ('id', 'add_time')
