# -*- coding: utf-8
from django import forms


class FileUploadForm(forms.Form):
    my_file = forms.FileField(label='文件名称:')
