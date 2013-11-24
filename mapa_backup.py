import psycopg2

dbname = "hackdown"
username = "crosschx_hack"
password = "Cr0sschx_H4ck"
host = "hack.crosschx.com"

conn = psycopg2.connect(database=dbname, user=username, password=password, host=host)

cur = conn.cursor()

departments = ["Pediatrics", "Emergency", "Cardiology", "Gynaecology", "Neurology", "Radiology"]

cur.execute("SELECT distinct t.location FROM transaction as t;")

hospitals = cur.fetchall()
total_hospitals = len(hospitals)

new_json = open('./new.json', 'w')

new_json.write('{"name": "CrossChx",\n "children": [\n')

for hospital_no in range(0, total_hospitals):
	hospital = hospitals[hospital_no]
	hospital_name = hospital[0]
	data = '{"name": "' + hospital_name + '",\n' + '"children": [\n'
	cur.execute("Select t.operation, count(t.encounter_id) FROM transaction as t Where t.location = (%s) GROUP BY t.operation", [hospital_name])
	transactions = cur.fetchall()
	total_transactions = len(transactions)
	for i in range(0, total_transactions):
		transaction = transactions[i]
		transaction_type = transaction[0]
		transaction_amount = transaction[1]
		data += '{"name": "' + transaction_type + '",\n' + '"children": [\n'
		cur.execute("Select p.first_name, p.last_name, tt.operation_time FROM transaction as t, patient as p, transaction_time as tt Where t.location = (%s) and p.puid = t.puid and t.operation = (%s) and t.encounter_id = tt.encounter_id", [hospital_name, transaction_type])
		patients = cur.fetchall()
		total_patients = (len(patients)^(i*6))/100
		for x in range(0, total_patients):
			patient = patients[x*2]
			first_name = patient[0]
			last_name = patient[1]
			encounters = int(patient[2][1:])*133%31
			patient_name = first_name + ' ' + last_name
			data += '{"name": "' + patient_name + '", ' + '"size": ' + str(encounters) + '}'
			if x != total_patients-1:
				data += ','
			data += '\n'
			
		data += ']\n'
		data += '}'
		if i != total_transactions-1:
			data += ',\n'

	data += ']\n'
	data += '}'
	if hospital_no != total_hospitals-1:
		data += ',\n'
	new_json.write(data)

new_json.write(']}')

cur.close()
conn.close()
