import uuid

from django.contrib.auth.models import User
from django.db import models

# Create your models here.
class Application(models.Model):
    """A list of treeherder api clients"""
    app_id = models.SlugField(max_length=32, unique=True)
    secret = models.CharField(max_length=32, default=uuid.uuid4, editable=False)
    description = models.TextField()
    owner = models.ForeignKey(User)
    authorized = models.BooleanField(default=False)
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'application'
