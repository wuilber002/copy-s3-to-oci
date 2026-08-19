#!/usr/bin/env python3
"""Build the deliberately simple, editable Raijin cost calculator workbook."""

from pathlib import Path

from openpyxl import Workbook
from openpyxl.formatting.rule import ColorScaleRule
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.worksheet.datavalidation import DataValidation

OUTPUT = Path(__file__).resolve().parents[1] / "docs" / "raijin-cost-calculator.xlsx"
NAVY, PANEL, BLUE, GREEN, WHITE, MUTED, INPUT = "101A2D", "19263E", "3B82F6", "166534", "F8FAFC", "C5D2E8", "FFF2CC"
THIN = Side(style="thin", color="4B5E7A")


def setup(ws, title, subtitle, columns):
    ws.sheet_view.showGridLines = False
    for row, text, size, color in ((1, title, 18, NAVY), (2, subtitle, 10, PANEL)):
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=columns)
        cell = ws.cell(row, 1, text)
        cell.font = Font(size=size, bold=row == 1, italic=row == 2, color=WHITE if row == 1 else MUTED)
        cell.fill = PatternFill("solid", fgColor=color)
        cell.alignment = Alignment(vertical="center", wrap_text=True)
        ws.row_dimensions[row].height = 32


def header(cell):
    cell.font = Font(bold=True, color=WHITE)
    cell.fill = PatternFill("solid", fgColor=BLUE)
    cell.alignment = Alignment(wrap_text=True, vertical="center")
    cell.border = Border(top=THIN, bottom=THIN, left=THIN, right=THIN)


def style(cell, editable=False):
    cell.fill = PatternFill("solid", fgColor=INPUT if editable else PANEL)
    cell.font = Font(color="111827" if editable else WHITE)
    cell.alignment = Alignment(vertical="center", wrap_text=True)
    cell.border = Border(top=THIN, bottom=THIN, left=THIN, right=THIN)


def money(cell): cell.number_format = 'US$ #,##0.00;[Red]-US$ #,##0.00;—'
def number(cell, decimals=2): cell.number_format = f'#,##0.{"0" * decimals};[Red]-#,##0.{"0" * decimals};—'


def build_transfer_data(wb):
    ws = wb.active
    ws.title = "Transfer data"
    setup(ws, "RAIJIN — Transfer data", "Edit only yellow cells. Use decimal units: 1 TB = 1,000 GB. Costs are calculated in the Summary sheet.", 3)
    for col, width in {"A": 42, "B": 25, "C": 78}.items(): ws.column_dimensions[col].width = width
    for c, label in enumerate(("Information", "Value", "Description"), 1): ws.cell(4, c, label); header(ws.cell(4, c))
    rows = [
        ("AWS region", "sa-east-1", "Select a region listed in ‘Tariffs by region’."),
        ("Total data to transfer (TB)", 600, "Total source data, in decimal TB."),
        ("Total files", 3600000, "Number of objects discovered in S3."),
        ("Wave size (TB)", 5, "Maximum payload per wave."),
        ("Calculated waves", "=ROUNDUP(B6/B8,0)", "Calculated automatically."),
        ("Deep Archive share", 1, "1 = all data is Deep Archive; 0.5 = half the data."),
        ("Restore tier", "BULK", "BULK or STANDARD."),
        ("Restore retention (days)", 2, "Temporary S3 Standard copy retention after restore is available."),
        ("Expected polling cycles per wave", 24, "Used for the conservative polling request estimate."),
        ("Include AWS outbound", True, "Turn off only if outbound is intentionally excluded."),
        ("Outbound tariff", "FastConnect", "Public AWS or FastConnect/Direct Connect tariff from the regional table."),
        ("Include OCI storage", True, "Includes OCI write requests, accrued storage and final recurring storage."),
        ("Estimated migration duration (days)", 47, "Used only to estimate OCI storage accumulated while data arrives."),
        ("OCI write operations per file", 4, "1 for small files; increase for multipart uploads."),
        ("Preserve S3 tags", True, "Adds one S3 GET/TAG request per file."),
        ("Early Deep Archive delete data (TB)", 0, "Volume deleted after migration; leave zero when data stays in S3."),
        ("Average Deep Archive age (days)", 180, "AWS charges the remaining portion of the 180-day commitment."),
    ]
    for r, data in enumerate(rows, 5):
        for c, value in enumerate(data, 1): ws.cell(r, c, value); style(ws.cell(r, c), editable=c == 2 and r != 9)
        ws.row_dimensions[r].height = 29
    for cell, decimals in (("B6", 2), ("B7", 0), ("B8", 2), ("B9", 0), ("B12", 0), ("B13", 0), ("B17", 0), ("B18", 2), ("B20", 2), ("B21", 0)): number(ws[cell], decimals)
    ws["B10"].number_format = "0.0%"
    yes_no = DataValidation(type="list", formula1='"TRUE,FALSE"'); tier = DataValidation(type="list", formula1='"BULK,STANDARD"'); outbound = DataValidation(type="list", formula1='"Public AWS,FastConnect"')
    ws.add_data_validation(yes_no); yes_no.add("B14"); yes_no.add("B16"); yes_no.add("B19")
    ws.add_data_validation(tier); tier.add("B11")
    ws.add_data_validation(outbound); outbound.add("B15")
    ws.conditional_formatting.add("B10", ColorScaleRule(start_type="num", start_value=0, start_color="F8696B", mid_type="num", mid_value=.5, mid_color="FFEB84", end_type="num", end_value=1, end_color="63BE7B"))
    ws.freeze_panes = "A5"


def build_tariffs(wb):
    ws = wb.create_sheet("Tariffs by region")
    setup(ws, "Tariffs by region", "Enter public or contractual values. The refresh script updates the AWS public columns for sa-east-1/us-east-1; circuit, Deep Archive storage and OCI values remain your inputs.", 15)
    headers = ["AWS region", "Currency", "AWS outbound\nUSD/GB", "FastConnect / Direct Connect\noutbound USD/GB", "Deep Archive storage\nUSD/GB-month", "Deep Archive BULK retrieval\nUSD/GB", "Deep Archive STANDARD retrieval\nUSD/GB", "Temporary S3 Standard restore\nUSD/GB-month", "S3 PUT/LIST\nUSD/1,000", "S3 GET/TAG\nUSD/1,000", "Batch job\nUSD/job", "Batch object\nUSD/1,000", "OCI Standard storage\nUSD/GB-month", "OCI writes\nUSD/10,000", "Notes / source"]
    widths = [16, 11, 15, 24, 20, 23, 27, 26, 17, 17, 15, 20, 22, 17, 46]
    for c, (label, width) in enumerate(zip(headers, widths), 1): ws.cell(4, c, label); header(ws.cell(4, c)); ws.column_dimensions[ws.cell(4, c).column_letter].width = width
    starter = [
        ["sa-east-1", "USD", .15, None, None, .008, .028, .0405, .007, .00056, .25, .000015, .0255, .0034, "AWS public values collected by Raijin on 2026-08-19; validate contracted rates before approval."],
        ["us-east-1", "USD", .09, None, None, .0025, .02, .0125, .055, .0004, .25, .001, .0255, .0034, "Example values; use the refresh script or contractual values."],
    ]
    for r in range(5, 55):
        values = starter[r - 5] if r <= 6 else [None] * 15
        for c, value in enumerate(values, 1): ws.cell(r, c, value); style(ws.cell(r, c), editable=True); money(ws.cell(r, c)) if 3 <= c <= 14 else None
    ws.auto_filter.ref = "A4:O54"; ws.freeze_panes = "A5"


def build_summary(wb):
    ws = wb.create_sheet("Summary")
    setup(ws, "Migration cost summary", "The one-time project cost excludes the recurring OCI monthly storage cost, shown separately at the bottom.", 7)
    headers = ["Metric", "Quantity", "Unit", "Rate used", "Rate source", "Estimated cost", "Calculation / note"]
    widths = [41, 19, 18, 18, 18, 21, 54]
    for c, (label, width) in enumerate(zip(headers, widths), 1): ws.cell(4, c, label); header(ws.cell(4, c)); ws.column_dimensions[ws.cell(4, c).column_letter].width = width
    rows = [
        ("Deep Archive BULK retrieval", "='Transfer data'!B8*1000*'Transfer data'!B10", "GB / wave", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,6,FALSE)", "AWS tariff", "=IF('Transfer data'!B11=\"BULK\",IFERROR(B5*D5,\"Not priced\"),0)", "Only the Deep Archive share."),
        ("Deep Archive STANDARD retrieval", "='Transfer data'!B8*1000*'Transfer data'!B10", "GB / wave", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,7,FALSE)", "AWS tariff", "=IF('Transfer data'!B11=\"STANDARD\",IFERROR(B6*D6,\"Not priced\"),0)", "Only the Deep Archive share."),
        ("Temporary restored copy", "='Transfer data'!B8*1000*'Transfer data'!B10*'Transfer data'!B12/30", "GB-month / wave", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,8,FALSE)", "AWS tariff", "=IFERROR(B7*D7,\"Not priced\")", "Temporary S3 Standard copy after restore."),
        ("S3 Batch Operations", "=1", "job / wave", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,11,FALSE)", "AWS tariff", "=IFERROR(B8*D8,\"Not priced\")", "One restore job per archive wave."),
        ("S3 Batch object tasks", "=ROUNDUP('Transfer data'!B7/'Transfer data'!B9,0)", "objects / wave", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,12,FALSE)/1000", "AWS tariff", "=IFERROR(B9*D9,\"Not priced\")", "Object-task rate normalized to one object."),
        ("Discovery + restore polling", "=(ROUNDUP('Transfer data'!B7/1000,0)*'Transfer data'!B13)+ROUNDUP('Transfer data'!B7/'Transfer data'!B9/1000,0)", "requests / wave", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,9,FALSE)/1000", "AWS tariff", "=IFERROR(B10*D10,\"Not priced\")", "Conservative ListObjectsV2 estimate."),
        ("S3 object reads", "=ROUNDUP('Transfer data'!B7/'Transfer data'!B9,0)", "requests / wave", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,10,FALSE)/1000", "AWS tariff", "=IFERROR(B11*D11,\"Not priced\")", "One streaming GetObject per object."),
        ("S3 tag reads", "=ROUNDUP('Transfer data'!B7/'Transfer data'!B9,0)", "requests / wave", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,10,FALSE)/1000", "AWS tariff", "=IF('Transfer data'!B19,IFERROR(B12*D12,\"Not priced\"),0)", "Only when tag preservation is enabled."),
        ("AWS outbound to OCI", "='Transfer data'!B8*1000", "GB / wave", "=IF('Transfer data'!B15=\"FastConnect\",IFERROR(IF(VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,4,FALSE)>0,VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,4,FALSE),VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,3,FALSE)),VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,3,FALSE)),VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,3,FALSE))", "Selected outbound tariff", "=IF('Transfer data'!B14,IFERROR(B13*D13,\"Not priced\"),0)", "FastConnect falls back to public AWS when its rate is blank."),
        ("Early Deep Archive deletion", "='Transfer data'!B20*1000*MAX(0,180-'Transfer data'!B21)/30", "GB-month / project", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,5,FALSE)", "AWS tariff", "=IF('Transfer data'!B20>0,IFERROR(B14*D14,\"Not priced\"),0)", "Whole-project early-delete estimate."),
        ("OCI write requests", "=ROUNDUP('Transfer data'!B7/'Transfer data'!B9,0)*'Transfer data'!B18", "operations / wave", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,14,FALSE)/10000", "OCI tariff", "=IF('Transfer data'!B16,IFERROR(B15*D15,\"Not priced\"),0)", "Set operations/file to 1 for small files; increase for multipart."),
        ("OCI storage during migration", "='Transfer data'!B6*1000*'Transfer data'!B17/2/30", "GB-month / project", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,13,FALSE)", "OCI tariff", "=IF('Transfer data'!B16,IFERROR(B16*D16,\"Not priced\"),0)", "Linear-arrival approximation over migration duration."),
        ("OCI final storage", "='Transfer data'!B6*1000", "GB-month / project", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:O,13,FALSE)", "OCI tariff", "=IF('Transfer data'!B16,IFERROR(B17*D17,\"Not priced\"),0)", "Recurring monthly destination cost; excluded from one-time total."),
    ]
    for r, values in enumerate(rows, 5):
        for c, value in enumerate(values, 1): ws.cell(r, c, value); style(ws.cell(r, c))
        number(ws.cell(r, 2), 4); money(ws.cell(r, 4)); money(ws.cell(r, 6))
    totals = [("One-time cost / wave", "=SUM(F5:F13)+F15"), ("One-time cost / project", "=F20*'Transfer data'!B9+F14+F16"), ("OCI recurring final cost / month", "=F17")]
    for r, (label, formula) in enumerate(totals, 20):
        ws.cell(r, 1, label); ws.cell(r, 6, formula)
        for c in range(1, 7): style(ws.cell(r, c))
        ws.cell(r, 1).font = Font(bold=True, color=WHITE); ws.cell(r, 6).font = Font(size=14, bold=True, color=WHITE); ws.cell(r, 6).fill = PatternFill("solid", fgColor=GREEN); money(ws.cell(r, 6))
    ws.freeze_panes = "A5"


def main():
    wb = Workbook()
    wb.calculation.fullCalcOnLoad = wb.calculation.forceFullCalc = True
    wb.calculation.calcMode = "auto"
    build_transfer_data(wb); build_tariffs(wb); build_summary(wb)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    wb.save(OUTPUT)
    print(OUTPUT)


if __name__ == "__main__": main()
