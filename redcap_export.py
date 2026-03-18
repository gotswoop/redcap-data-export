#!/usr/bin/env python3

"""
redcap_export.py — REDCap Data Export Utility

Description:
	Export data from a REDCap project directly from the MySQL database.
	Supports CSV or XML output, optional label conversion, gzip compression,
	and configurable progress reporting.

Prerequisites:
	- Configure MySQL credentials in ~/.my.cnf for the user running this script.
	- Set the target database name in the DB variable defined in this script.

Usage:
	./redcap_export.py <project_id> [options]

Options:
	--format csv|xml		Output format (default: csv)
	--labels raw|label		Export raw values or human-readable labels (default: raw)
	--gzip					Compress output using gzip
	--progress-interval N	Print progress every N records processed
	--export-metadata		Export project metadata only (no data)
	--metadata-format csv|xml	Metadata output format (default: csv)

Examples:
	./redcap_export.py 123
	./redcap_export.py 123 --labels label --gzip
	./redcap_export.py 123 --format xml --progress-interval 2000

Author:
	Swaroop Samek

Date:
	March 2026
"""

from __future__ import print_function
import argparse
import subprocess
import csv
import time
import sys
import gzip
import math
import signal

# CONFIG: adjust DB name if needed
DB = "redcap_prod"

# Helper: run mysql and return stdout (throws CalledProcessError on failure)
def run_mysql(query):
	cmd = ["mysql", DB, "-N", "-B", "-e", query]
	return subprocess.check_output(cmd).decode()

# Helper: open streaming mysql process
def mysql_stream_proc(query):
	cmd = ["mysql", DB, "-N", "-B", "-e", query]
	return subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)

# Small helper to parse element_enum -> [(code,label),...]
def parse_element_enum(enum_str):
	if not enum_str:
		return []
	items = []
	for chunk in enum_str.split("|"):
		chunk = chunk.strip()
		if chunk == "":
			continue
		if "," in chunk:
			code, label = chunk.split(",", 1)
			items.append((code.strip(), label.strip()))
		else:
			items.append((chunk.strip(), chunk.strip()))
	return items

# Print progress on stderr (so stdout can be piped)
def print_progress(processed_rows, rows_total, start_time, last_time, last_count):
	elapsed = time.time() - start_time
	if elapsed <= 0:
		elapsed = 0.0001
	rps = processed_rows / elapsed
	percent = None
	eta = None
	if rows_total and rows_total > 0:
		percent = processed_rows / float(rows_total) * 100.0
		remaining = max(0, rows_total - processed_rows)
		eta = remaining / rps if rps > 0 else None
	# simple rate smoothing using last_count/last_time where available omitted for brevity
	if eta is None:
		eta_str = "N/A"
	else:
		eta_str = "%s s" % int(eta)
	if percent is None:
		pct_str = ""
	else:
		pct_str = " (%5.1f%%)" % percent
	sys.stderr.write("\rProcessed rows: %d%s | %.1f rows/sec | elapsed: %ds | ETA: %s" % (
		processed_rows, pct_str, rps, int(elapsed), eta_str))
	sys.stderr.flush()

def signal_handler(sig, frame):
	sys.stderr.write("\nExport interrupted by user (signal %s)\n" % sig)
	sys.exit(1)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ---------------------------
# CLI
# ---------------------------
parser = argparse.ArgumentParser(description="Polished REDCap exporter (streaming, gzip, progress)")
parser.add_argument("pid", type=int, help="REDCap project_id")
parser.add_argument("--format", choices=["csv", "xml"], default="csv", help="output format")
parser.add_argument("--labels", choices=["raw", "label"], default="raw", help="export raw codes or labels")
parser.add_argument("--gzip", action="store_true", help="gzip compress the output file")
parser.add_argument("--progress-interval", type=int, default=2000, help="update progress every N rows processed")
parser.add_argument("--export-metadata", action="store_true", help="export project metadata only")
parser.add_argument("--metadata-format", choices=["csv","xml"], default="csv", help="metadata output format")
args = parser.parse_args()

pid = args.pid
OUT_FORMAT = args.format
LABEL_MODE = args.labels
USE_GZIP = args.gzip
PROGRESS_INTERVAL = max(1, args.progress_interval)

label_mode_str = "raw_data" if LABEL_MODE == "raw" else "labeled_data"
outfile_base = "project_%d_%s.%s" % (pid, label_mode_str, "xml" if OUT_FORMAT == "xml" else "csv")
outfile = outfile_base + (".gz" if USE_GZIP else "")

start_time = time.time()

print("\nREDCap Export Summary")
print("----------------------")

# ---------------------------
# METADATA EXPORT ONLY
# ---------------------------
if args.export_metadata:
	meta_query = """
	SELECT field_name, form_name, IFNULL(element_type,''), REPLACE(REPLACE(IFNULL(element_enum,''),'\\n',' '),'\\r',' ')
	FROM redcap_metadata
	WHERE project_id=%d
	ORDER BY field_order;
	""" % pid

	meta_raw = run_mysql(meta_query)

	outfile_meta = "project_%d_metadata.%s" % (pid, "xml" if args.metadata_format == "xml" else "csv")

	if args.metadata_format == "csv":
		with open(outfile_meta, "w", newline="") as f:
			writer = csv.writer(f)
			writer.writerow(["field_name","form_name","element_type","element_enum"])
			for line in meta_raw.splitlines():
				writer.writerow(line.split("\t"))
	else:
		with open(outfile_meta, "w") as f:
			f.write('<?xml version="1.0" encoding="UTF-8" ?>\n<project>\n')
			current_form = None
			for line in meta_raw.splitlines():
				parts = line.split("\t")
				if len(parts) < 4:
					continue
				field, form, etype, enum = parts
				if form != current_form:
					if current_form is not None:
						f.write("\t</instrument>\n")
					f.write("\t<instrument name=\"%s\">\n" % form)
					current_form = form
				f.write("\t\t<field name=\"%s\" type=\"%s\">\n" % (field, etype))
				if enum:
					for choice in enum.split("|"):
						if "," in choice:
							c,l = choice.split(",",1)
							f.write("\t\t\t<choice code=\"%s\" label=\"%s\"/>\n" % (c.strip(), l.strip()))
				f.write("\t\t</field>\n")
			if current_form is not None:
				f.write("\t</instrument>\n")
			f.write("</project>\n")

	print("Metadata export complete")
	print("Output file:", outfile_meta)
	sys.exit(0)

# ---------------------------
# Determine data table for project
# ---------------------------
try:
	data_table = run_mysql("SELECT data_table FROM redcap_projects WHERE project_id=%d;" % pid).strip()
except subprocess.CalledProcessError as e:
	sys.stderr.write("ERROR running mysql to find data_table: %s\n" % e)
	sys.exit(2)

if not data_table:
	data_table = "redcap_data"

print("Using data table:", data_table)

# ---------------------------
# Load metadata (safe sanitize newlines in enums)
# ---------------------------
print("Loading metadata...")

meta_query = """
SELECT field_name, form_name, IFNULL(element_type,''), REPLACE(REPLACE(IFNULL(element_enum,''),'\\n',' '),'\\r',' ')
FROM redcap_metadata
WHERE project_id=%d
ORDER BY field_order;
""" % pid

try:
	meta_raw = run_mysql(meta_query)
except subprocess.CalledProcessError as e:
	sys.stderr.write("ERROR retrieving metadata: %s\n" % e)
	sys.exit(3)

metadata = []
field_to_form = {}
field_element_type = {}
field_element_enum = {}

for line in meta_raw.splitlines():
	parts = line.split("\t")
	if len(parts) < 1:
		continue
	field = parts[0]
	form = parts[1] if len(parts) > 1 else ""
	etype = parts[2] if len(parts) > 2 else ""
	enum = parts[3] if len(parts) > 3 else ""
	metadata.append(field)
	field_to_form[field] = form
	field_element_type[field] = etype
	field_element_enum[field] = enum

if not metadata:
	sys.stderr.write("No metadata found for project %d\n" % pid)
	sys.exit(4)

record_id_field = metadata[0]

# Expand checkbox fields
expanded_fields = []
checkbox_choices = {}	# field -> [codes]
choice_label_map = {}	# field -> {code:label}
for f in metadata:
	if field_element_type.get(f) == "checkbox":
		parsed = parse_element_enum(field_element_enum.get(f, ""))
		codes = [c for (c,l) in parsed]
		labels = {c:l for (c,l) in parsed}
		checkbox_choices[f] = codes
		choice_label_map[f] = labels
		for code in codes:
			expanded_fields.append("%s___%s" % (f, code))
	else:
		expanded_fields.append(f)

# ---------------------------
# Summary stats
# ---------------------------
try:
	rows_total = int(run_mysql("SELECT COUNT(*) FROM %s WHERE project_id=%d;" % (data_table, pid)).strip() or "0")
	records_total = int(run_mysql("SELECT COUNT(*) FROM %s WHERE project_id=%d AND field_name='%s';" % (data_table, pid, record_id_field)).strip() or "0")
except subprocess.CalledProcessError as e:
	sys.stderr.write("ERROR retrieving counts: %s\n" % e)
	sys.exit(5)

print("Project ID:", pid)
print("Database rows: %s" % format(rows_total, ","))
print("Project records: %s" % format(records_total, ","))
print("Instruments: %s" % len(set(field_to_form.values())))
print("Fields: %s" % len(metadata))
print("Checkbox fields: %s" % len(checkbox_choices))
print("Output file:", outfile)
print("Format:", OUT_FORMAT, "| Labels:", LABEL_MODE, "| Gzip:", USE_GZIP)
print("Starting streaming export... (this may take minutes for large projects)")

# ---------------------------
# Events mapping
# ---------------------------
events = {}
try:
	ev_q = """
	SELECT e.event_id, CONCAT(a.arm_num, '_', e.descrip)
	FROM redcap_events_metadata e
	JOIN redcap_events_arms a ON a.arm_id = e.arm_id
	WHERE a.project_id=%d;
	""" % pid
	for line in run_mysql(ev_q).splitlines():
		parts = line.split("\t")
		if len(parts) == 2:
			events[parts[0]] = parts[1]
except subprocess.CalledProcessError:
	events = {}

# ---------------------------
# Repeating instruments (best-effort)
# ---------------------------
repeat_instrument_map = {}
try:
	rep_q = """
	SELECT r.event_id, r.form_name
	FROM redcap_events_repeat r
	JOIN redcap_events_metadata e ON e.event_id = r.event_id
	JOIN redcap_events_arms a ON a.arm_id = e.arm_id
	WHERE a.project_id=%d;
	""" % pid
	for line in run_mysql(rep_q).splitlines():
		parts = line.split("\t")
		if len(parts) >= 2:
			repeat_instrument_map[parts[0]] = parts[1]
except subprocess.CalledProcessError:
	repeat_instrument_map = {}

# ---------------------------
# DAG labels
# ---------------------------
dag_map = {}
try:
	dag_q = "SELECT group_id, group_name FROM redcap_data_access_groups WHERE project_id=%d;" % pid
	for line in run_mysql(dag_q).splitlines():
		parts = line.split("\t")
		if len(parts) >= 2:
			dag_map[parts[0]] = parts[1]
except subprocess.CalledProcessError:
	dag_map = {}

# ---------------------------
# Prepare output writer (csv or xml), support gzip
# ---------------------------
open_f = open
if USE_GZIP:
	def open_f(path, mode="w", newline=None):
		# force text mode for gzip
		if "b" not in mode:
			mode = mode.replace("w", "wt")
		return gzip.open(path, mode, encoding="utf-8")

if OUT_FORMAT == "csv":
	fout = open_f(outfile, "w", newline="")
	csv_writer = csv.writer(fout)
	header = [record_id_field, "redcap_event_name", "redcap_repeat_instrument", "redcap_repeat_instance", "redcap_data_access_group"] + expanded_fields
	csv_writer.writerow(header)
else:
	fout = open_f(outfile, "w")
	fout.write('<?xml version="1.0" encoding="UTF-8" ?>\n<records>\n')

# ---------------------------
# Stream data rows and pivot per (record,event,instance)
# ---------------------------
data_q = """
SELECT record,event_id,field_name,value,IFNULL(instance,1)
FROM %s
WHERE project_id=%d
ORDER BY record,event_id,instance;
""" % (data_table, pid)

proc = mysql_stream_proc(data_q)

current_key = None
row_map = {}	# holds current record/event/instance's flattened values
processed_rows = 0
processed_records = 0
last_progress_time = time.time()
last_processed = 0

# helper to emit a row
def emit_current(key, row_map_local):
	global csv_writer, fout
	record, event, inst = key
	event_name = events.get(event, "")
	repeat_name = repeat_instrument_map.get(event, "")
	dag_label = ""
	# try to pull group id if present in row_map_local
	gid = row_map_local.get("__GROUPID__") or row_map_local.get("__groupid__") or row_map_local.get("__GROUPID__", "")
	if gid:
		dag_label = dag_map.get(gid, "")
	if OUT_FORMAT == "csv":
		outrow = [record, event_name, repeat_name, inst, dag_label]
		for col in expanded_fields:
			val = row_map_local.get(col, "")
			# if label mode and not a checkbox expanded column, map single-choice
			if LABEL_MODE == "label" and "___" not in col:
				base = col
				if base in choice_label_map and val in choice_label_map[base]:
					val = choice_label_map[base][val]
			outrow.append(val if val is not None else "")
		csv_writer.writerow(outrow)
	else:
		# buffered xml writing: collect lines then write once
		xml_lines = []
		xml_lines.append("\t<item>")
		# system cols
		fields_to_write = [record_id_field, "redcap_event_name", "redcap_repeat_instrument", "redcap_repeat_instance", "redcap_data_access_group"] + expanded_fields
		values = [record, event_name, repeat_name, inst, dag_label]
		for col in expanded_fields:
			val = row_map_local.get(col, "")
			if LABEL_MODE == "label" and "___" not in col:
				base = col
				if base in choice_label_map and val in choice_label_map[base]:
					val = choice_label_map[base][val]
			values.append(val if val is not None else "")
		for colname, val in zip(fields_to_write, values):
			# use CDATA for safe text; val must be string
			if val is None:
				val = ""
			xml_lines.append("\t<%s><![CDATA[%s]]></%s>" % (colname, str(val), colname))
		xml_lines.append("\t</item>\n")
		fout.write("\n".join(xml_lines))

# Main loop
try:
	for rawline in proc.stdout:
		parts = rawline.rstrip("\n").split("\t")
		if len(parts) < 5:
			# skip malformed line
			continue
		record, event, field, value, inst = parts[0], parts[1], parts[2], parts[3], parts[4]
		if inst == "":
			inst = "1"
		key = (record, event, inst)
		# If we've moved to a new key, flush previous
		if current_key is not None and key != current_key:
			emit_current(current_key, row_map)
			processed_records += 1
			row_map = {}
		# Handle checkbox expansions:
		# If the incoming field is a parent checkbox name (e.g., 'symptom') and value indicates code or '1',
		# try to set corresponding expanded columns. Also accept fields already in expanded 'field___code' form.
		if "___" in field:
			# expanded already
			row_map[field] = value
		else:
			# micro-optimized lookup
			codes = checkbox_choices.get(field)
			if codes:
				# parent checkbox field - REDCap sometimes stores 'field' rows with value '1' per checked code, but not always
				# We attempt: if value corresponds to a code (rare), set that code; if value == '1' and only one code, set it.
				if value in codes:
					row_map["%s___%s" % (field, value)] = "1"
				elif value == "1" and len(codes) == 1:
					row_map["%s___%s" % (field, codes[0])] = "1"
				else:
					# can't map reliably; store parent field raw (for completeness)
					row_map[field] = value
			else:
				# normal field
				row_map[field] = value
		current_key = key
		processed_rows += 1
		# progress update
		if processed_rows % PROGRESS_INTERVAL == 0:
			print_progress(processed_rows, rows_total, start_time, last_progress_time, last_processed)
			last_progress_time = time.time()
			last_processed = processed_rows
	# flush last
	if current_key is not None:
		emit_current(current_key, row_map)
		processed_records += 1
finally:
	# Cleanup
	try:
		proc.stdout.close()
	except Exception:
		pass
	ret = proc.wait()
	if ret != 0:
		sys.stderr.write("\nmysql process exited with code %d\n" % ret)

# Close output
if OUT_FORMAT == "csv":
	fout.close()
else:
	fout.write("</records>\n")
	fout.close()

# Final stats
print_progress(processed_rows, rows_total, start_time, last_progress_time, last_processed)
print("\n\nExport complete")
print("Output file:", outfile)
print("Rows processed (redcap_data rows scanned): %s" % format(processed_rows, ","))
print("Records written (record,event,instance rows): %s" % format(processed_records, ","))
print("Runtime: %.1f seconds" % (time.time() - start_time))
