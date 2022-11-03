# Déployer et exécuter un job Spark

## Packaging

Pour déployer une application Spark, il faut d'abord packager son code. Avec poetry il existe une commande pour faire ça :

```shell
poetry build
```

Pour cela il faut modifier le fichier `pyproject.toml` et indiquer comment packager son code. Voici le lien vers la doc poetry pour [configurer le packaging](https://python-poetry.org/docs/pyproject/#packages).

## Local

Pour exécuter son job Spark, un outil très pratique existe : `spark-submit`.

Petit problème : Poetry génère des packages python au format tar.gz ou wheel alors que `spark-submit` n'accepte que des zip ou des egg. Alors pourquoi s'être embêté à avoir configurer poetry ?

Il existe d'autres outils plus moderne pour lancer des jobs Spark compatible avec les wheels qu'on verra après.

Pour exécuter en local votre job Spark :

```shell
spark-submit --master local[*] spark-app.py arg1 arg2
```

Cet outil permet également de déployer sur un cluster Hadoop ou Kubernetes. Mais depuis l'arrivée des services managés cloud on ne l'utilise plus. 

## Cloud

### AWS Glue

Glue est un service managé sur AWS pour faire exécuter des jobs Spark serverless. C'est le plus simple à utiliser, en contre-partie c'est très rigide niveau configuration. Les seules exécuteurs utilisables font 4 CPUs et 16 Go de RAM (1 DPU) ou 2 DPU.

#### Infra as code

1. Créer un bucket S3

Pour stocker notre code et notre donnée, nous allons avoir besoin d'un bucket S3. Dans le fichier `infra/bucket.tf`, créez votre bucket :

```terraform
resource "aws_s3_bucket" "bucket" {
  bucket = "my-bucket"

  tags = local.tags
}
```

Pensez à bien nommer votre bucket sinon vous aurez une erreur comme quoi celui-ci existe déjà.

2. Créer les permissions pour votre job glue avec IAM

Pour donner le droit à votre job Glue d'accéder à votre bucket S3, vous devez créer un role IAM et une policy IAM. Dans le fichier `infra/iam.tf`, créez vos ressources IAM :

```terraform
data "aws_iam_policy_document" "glue-assume-role-policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
  
  statement {
    actions = [
      "s3:*",
      "kms:*",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role" "glue" {
  name               = "glue_role"
  assume_role_policy = data.aws_iam_policy_document.glue-assume-role-policy.json
}
```

Pensez à bien renommer vos ressources

3. Créer son job Glue

Maintenant nous pouvons créer notre job Glue et lui attribuer son role IAM et le chemin vers notre application Spark dans S3. 

```terraform
resource "aws_glue_job" "example" {
  name     = "example"
  role_arn = aws_iam_role.example.arn

  command {
    script_location = "s3://${aws_s3_bucket.example.bucket}/example.py"
  }
  
  default_arguments = {
    '--additional-python-modules': "s3://${aws_s3_bucket.example.bucket}/prefix/lib_A.whl"
  }
}
```

Pensez à bien remplacer les valeurs.

#### Déployer

1. [Installer terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
2. Créer le fichier `~/.aws/credentials`

```shell
[esgi-spark-core]
aws_access_key_id = ???
aws_secret_access_key = ???
```

3. Déclarer 2 variables d'environnement :

```shell
export AWS_PROFILE=esgi-spark-core
export AWS_REGION=eu-west-1
```

4. Déployer avec terraform

```shell
cd infra
terraform init
terraform apply
```

5. Uploader son package python dans S3

[Précédent](exo4.md) <-
