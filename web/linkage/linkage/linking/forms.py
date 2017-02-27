from django import forms
from django.forms.models import inlineformset_factory
from django.forms import (ModelForm,
                          Textarea,
                          TextInput,
                          Select,
                          ModelChoiceField,
                          HiddenInput,
                          ChoiceField,
                          CheckboxInput)

from django.utils.translation import ugettext_lazy as _
from .models import LinkingProject, LinkingStep, PROJECT_TYPES
from linkage.datasets.models import Dataset

TYPE_CHOICES = (('', 'Select Project Type'),) + PROJECT_TYPES


class ProjectTypeForm(forms.Form):
    type = ChoiceField(
        label='Project Type',
        choices=TYPE_CHOICES,
        widget=Select(attrs={'class': 'form-control'}))


class ProjectForm(ModelForm):
    left_data = ModelChoiceField(queryset=Dataset.objects.all(), label='Left',
                                 widget=Select(attrs={'class': 'form-control'}))

    class Meta:
        model = LinkingProject
        fields = ['name', 'description']

        labels = {
            'name': _('Project Name'),
            'description': _('Description'),
        }
        widgets = {
            'name': TextInput(attrs={'class': 'form-control'}),
            'description': Textarea(attrs={'rows': 5, 'class': 'form-control'}),
        }

    def __init__(self, *args, **kwargs):
        super(ProjectForm, self).__init__(*args, **kwargs)
        self.fields['left_data'].queryset = Dataset.objects.all()

        if self.instance.pk:
            try:
                data = LinkingProject.objects.get(pk=self.instance.pk)
                self.fields['left_data'].initial = data.linkingdataset_set.get(link_seq=1).dataset.pk
            except LinkingProject.DoesNotExist:
                pass


class LinkingForm(ProjectForm):
    right_data = ModelChoiceField(queryset=Dataset.objects.all(), label='right',
                                  widget=Select(attrs={'class': 'form-control'}))

    class Meta(ProjectForm.Meta):
        fields = ProjectForm.Meta.fields + ['relationship_type']

        labels = ProjectForm.Meta.labels
        labels['relationship_type'] = _('Entity Relationship Type')

        widgets = ProjectForm.Meta.widgets
        widgets['relationship_type'] = Select(attrs={'class': 'form-control'})

    def __init__(self, *args, **kwargs):
        super(LinkingForm, self).__init__(*args, **kwargs)
        self.fields['right_data'].queryset = Dataset.objects.all()

        # Get the list of dataset columns to fill index_field select box
        if self.instance.pk:
            try:
                data = LinkingProject.objects.get(pk=self.instance.pk)
                self.fields['right_data'].initial = data.linkingdataset_set.get(link_seq=2).dataset.pk
            except LinkingProject.DoesNotExist:
                pass


class DedupForm(ProjectForm):
    left_data = ModelChoiceField(queryset=Dataset.objects.all(), label='Data file',
                                 widget=Select(attrs={'class': 'form-control'}))

    class Meta(ProjectForm.Meta):
        fields = ProjectForm.Meta.fields

        labels = ProjectForm.Meta.labels

        widgets = ProjectForm.Meta.widgets


LinkingStepFormset = inlineformset_factory(
    LinkingProject,
    LinkingStep,
    fields=('seq', 'blocking_schema', 'linking_schema', 'group'),
    widgets={
        'seq': HiddenInput(attrs={'class': 'step-seq form-control'}),
        'blocking_schema': HiddenInput(),
        'linking_schema': HiddenInput(),
        'group': CheckboxInput(attrs={'class': 'ios-toggle toggle-info form-control'})
    },
    labels={
        'seq': _('Sequence'),
        'group': _('Group Records?'),
    },
    extra=0
)
