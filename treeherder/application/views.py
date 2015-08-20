from django.core.urlresolvers import reverse_lazy
from django.shortcuts import get_object_or_404
from django.views.generic.edit import CreateView, DeleteView, UpdateView
from django.views.generic import DetailView, ListView

from .models import Application


class ApplicationList(ListView):
    model = Application

    def get_queryset(self):
        return Application.objects.filter(owner=self.request.user).order_by('-created')


class ApplicationCreate(CreateView):
    model = Application
    fields = ['app_id', 'description']
    success_url = reverse_lazy('application-list')

    def form_valid(self, form):
        form.instance.owner = self.request.user
        return super(ApplicationCreate, self).form_valid(form)


class ApplicationDetail(DetailView):
    model = Application

    def get_queryset(self):
        return Application.objects.filter(owner=self.request.user)


class ApplicationDelete(DeleteView):
    model = Application
    success_url = reverse_lazy('application-list')
