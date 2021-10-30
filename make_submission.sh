submission_name=$(date +"%Y-%m-%d_%H-%M-%S")
echo $submission_name
PYTHONPATH=. SUB_NAME=$submission_name python make_submission.py | tee ../data/logs/$submission_name.log || exit 1
git archive -v --format=zip HEAD > ../data/submissions/$submission_name.zip
#git tag -a $submission_name -m "$submission_name"
