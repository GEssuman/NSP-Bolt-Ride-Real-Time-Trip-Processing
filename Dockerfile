FROM public.ecr.aws/glue/aws-glue-libs:5

# Install delta-spark
RUN pip install --no-cache-dir delta-spark
