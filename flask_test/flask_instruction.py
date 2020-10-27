# https://www.datacamp.com/community/tutorials/machine-learning-models-api-python

call API Postman :
POST: 
localhost:12345/predict

Body:
[
	{"Age":85, "Sex": "male", "Embarked":"S"},
	{"Age":24, "Sex": "male", "Embarked":"C"},
	{"Age":82, "Sex": "female", "Embarked":"C"}
	]


Result exmaple:
{
  "prediction": "[0, 0, 1]"
}
