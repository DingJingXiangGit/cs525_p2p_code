import json
import sys

config = {
	"1280x800":{"x_base": 10, "y_base": 120, "offset":23, "scale":1.04},
	"1440x900":{"x_base": 12, "y_base": 135, "offset":26, "scale":1.17},
}


def main(resolution, infile, outfile):
	jsonFile = infile + ".json"
	tojson(infile, jsonFile)
	tranfer(resolution, jsonFile, outfile)

def tojson(infile, outfile):
	str_list = []
	counter = 0;
	with open(infile) as fp:
		for line in fp:
			counter += 1
			if counter == 1:
				continue;

			line = line.strip()
			tokens = line.split("\t")
			length = len(tokens)

			if length == 1:
				if tokens[0] == '}':
					str_list = str_list[0:-1]
					str_list.append(tokens[0])
					str_list.append(",\n")

				if tokens[0] != '}' and tokens[0] != '{':
					str_list.append(tokens[0])
					str_list.append(":")

				if tokens[0] == '{':
					str_list.append(tokens[0])
			else:

				str_list.append(tokens[0])
				str_list.append(":")
				str_list.append(tokens[2][1:-1])
				str_list.append(",")

		str_list = str_list[0:-1]
		out = "".join(str_list)
		outjson = json.loads(out)

		with open(outfile, 'w') as ofp:
			json.dump(outjson, ofp)
		

def tranfer(resolution, jsonFile, new_coordinate):
	heros = json.load(open(jsonFile))
	setting = config[resolution]
	new_heros = {}
	x_base = setting["x_base"] #10
	y_base = setting["y_base"] #120
	scale = setting["scale"]   #1.04
	offset = setting["offset"] #23
	for eid in heros:
		x = heros[eid]["x"] 
		y = heros[eid]["y"]
		heroId = heros[eid]["HeroID"]

		x_center = x * scale + x_base
		x_left = x_center - offset
		y_top = y * scale + y_base
		new_heros[heroId] = {}
		new_heros[heroId]["x"] = x_left
		new_heros[heroId]["y"] = y_top

	with open(new_coordinate, 'w') as ofp:
			json.dump(new_heros, ofp)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])

