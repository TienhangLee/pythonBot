# Generated by Django 3.2.9 on 2022-03-17 06:17

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='equityValue',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('customerId', models.CharField(max_length=200)),
                ('totalEquity', models.CharField(max_length=200)),
                ('timestamp', models.CharField(max_length=200)),
            ],
        ),
    ]
