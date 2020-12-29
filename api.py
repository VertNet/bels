from flask import Flask, request
import bels
app = Flask(__name__)

@app.route('/api/csv', methods=['POST'])
def csv():
    csv_content = request.get_data()
    csv_content = csv_content.decode('utf-8')
    lines = bels.read_from_csv(content=csv_content)

    output = ''
    for line in lines:
        output += line.hash() + '\n'

    return output


if __name__ == "__main__":
	app.run(debug=True)

# need an virtualenv to test
# virtualenv --python=python3 env - only the 1st time
# source env/bin/activate
# pip install flask - only the 1st time
# python3 api.py
# curl -v --data-binary @test.csv http://127.0.0.1:5000/api/csv