from django.db import models
from django.contrib.postgres.fields import JSONField
from django_zombodb.indexes import ZomboDBIndex
from django_zombodb.querysets import SearchQuerySet
import uuid


# Create your models here.
class BaseManager(models.Manager):
    """
    The manager for the auth's Group model.
    """

    def query(self, *args, **kwargs):
        """
        获取有效记录
        """
        kwargs['enable_flag'] = True
        return self.filter(*args, **kwargs)


class BaseModel(models.Model):
    id = models.CharField(max_length=255, primary_key=True, default=uuid.uuid4, editable=False)
    sys_add_time = models.DateTimeField(auto_now_add=True)
    sys_check_time = models.DateTimeField(auto_now_add=True)
    sys_dm_update_time = models.DateTimeField(auto_now=True)
    enable_flag = models.BooleanField(default=True)

    objects = BaseManager()

    class Meta:
        abstract = True

    def delete(self, using=None, keep_parents=False):
        self.enable_flag = False
        self.save(using=using)

    def copy_model_instance(self, **kwargs):
        """Create a copy of a model instance.
        M2M relationships are currently not handled, i.e. they are not
        copied.
        See also Django #4027.
        """
        initial = dict([(f.name, getattr(self, f.name))
                        for f in self._meta.fields
                        if not isinstance(f, models.AutoField) and
                        f not in self._meta.parents.values() and
                        f.unique is False])
        initial.update(kwargs)
        return self.__class__(**initial)
