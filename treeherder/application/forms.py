from django.forms import ModelForm, TextArea, TextInput

from .models import Application

class ApplicationForm(ModelForm):

    class Meta:
        model = Application
        fields = ('app_id', 'description')
        widgets = {
            'app_id': TextInput(attrs={'class': 'form-control'}),
            'description': TextArea(attrs={'class': 'form-control'})
        }
