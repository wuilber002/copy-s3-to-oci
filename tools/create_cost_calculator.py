#!/usr/bin/env python3
"""Build the deliberately simple, editable Raijin cost calculator workbook."""

from pathlib import Path

from openpyxl import Workbook
from openpyxl.formatting.rule import ColorScaleRule
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.worksheet.datavalidation import DataValidation

OUTPUT = Path(__file__).resolve().parents[1] / "docs" / "raijin-cost-calculator.xlsx"
NAVY, PANEL, BLUE, GREEN, WHITE, MUTED, INPUT, TOTAL = "101A2D", "19263E", "3B82F6", "166534", "F8FAFC", "C5D2E8", "FFF2CC", "0F5132"
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


def money(cell): cell.number_format = '$ #,##0.00;[Red]-$ #,##0.00;$ 0.00'
def rate(cell): cell.number_format = '$ #,##0.000000;[Red]-$ #,##0.000000;$ 0.000000'
def number(cell, decimals=2):
    decimal_part = f'.{"0" * decimals}' if decimals else ''
    cell.number_format = f'#,##0{decimal_part};[Red]-#,##0{decimal_part};0'
def localized_formula(value):
    return value.replace("'Transfer data'", "'Dados da transferência'").replace("'Tariffs by region'", "'Tarifas por região'") if isinstance(value, str) else value


def build_transfer_data(wb):
    ws = wb.active
    ws.title = "Dados da transferência"
    setup(ws, "RAIJIN — Dados da transferência", "Preencha somente as células amarelas. Use unidades decimais: 1 TB = 1.000 GB. Os custos são calculados automaticamente na aba Resumo.", 3)
    for col, width in {"A": 42, "B": 25, "C": 78}.items(): ws.column_dimensions[col].width = width
    for c, label in enumerate(("Informação", "Valor", "O que informar e impacto no cálculo"), 1): ws.cell(4, c, label); header(ws.cell(4, c))
    rows = [
        ("Região AWS", "sa-east-1", "Informe o código da região do bucket de origem, por exemplo sa-east-1 ou us-east-1. Deve existir uma linha correspondente na aba Tarifas por região."),
        ("Dados totais a transferir (TB)", 600, "Informe todo o volume da origem em TB decimais. Exemplo: 600 significa 600.000 GB. É usado no custo total e na quantidade de waves."),
        ("Quantidade total de arquivos", 3600000, "Informe a quantidade de objetos do discovery S3. Ela determina as estimativas de requests, Batch Operations e polling."),
        ("Tamanho máximo da wave (TB)", 5, "Informe o limite de dados por wave. A planilha arredonda a quantidade de waves para cima; a última pode ser menor."),
        ("Quantidade calculada de waves", "=ROUNDUP(B6/B8,0)", "Campo calculado automaticamente a partir dos dados totais e do tamanho máximo da wave. Não editar."),
        ("Percentual em Deep Archive", 1, "Informe uma fração entre 0 e 1. Use 1 para 100% dos dados em DEEP_ARCHIVE, 0,5 para 50%. Apenas essa parcela recebe custos de restore."),
        ("Tier de restore", "BULK", "Selecione BULK para menor custo e maior prazo, ou STANDARD para maior velocidade e custo. Aplica-se à parcela Deep Archive."),
        ("Retenção do restore (horas)", 48, "Informe por quantas horas a cópia temporária S3 Standard ficará disponível após o restore terminar. Para Deep Archive BULK, 48 horas é a opção mais segura."),
        ("Ciclos esperados de polling por wave", 24, "Informe quantas verificações de disponibilidade a plataforma deve fazer, em média, até o restore ficar pronto. É uma estimativa conservadora de requests."),
        ("Preservar tags S3", True, "Use TRUE quando as tags S3 devem ser copiadas. Isso adiciona uma leitura GET/TAG por arquivo. Use FALSE quando tags não são necessárias."),
        ("Dados Deep Archive a excluir antecipadamente (TB)", 0, "Informe apenas o volume que será excluído ou movido de Deep Archive antes de completar 180 dias. Use zero se os dados permanecerem no S3."),
        ("Idade média no Deep Archive (dias)", 180, "Informe a idade média ponderada desde a entrada no Deep Archive — não use LastModified se a transição foi por lifecycle. Com 180 ou mais, não há cobrança antecipada."),
    ]
    for r, data in enumerate(rows, 5):
        for c, value in enumerate(data, 1): ws.cell(r, c, value); style(ws.cell(r, c), editable=c == 2 and r != 9)
        ws.row_dimensions[r].height = 29
    for cell, decimals in (("B6", 2), ("B7", 0), ("B8", 2), ("B9", 0), ("B12", 0), ("B13", 0), ("B15", 2), ("B16", 0)): number(ws[cell], decimals)
    ws["B10"].number_format = "0.0%"
    yes_no = DataValidation(type="list", formula1='"TRUE,FALSE"'); tier = DataValidation(type="list", formula1='"BULK,STANDARD"')
    ws.add_data_validation(yes_no); yes_no.add("B14")
    ws.add_data_validation(tier); tier.add("B11")
    ws.conditional_formatting.add("B10", ColorScaleRule(start_type="num", start_value=0, start_color="F8696B", mid_type="num", mid_value=.5, mid_color="FFEB84", end_type="num", end_value=1, end_color="63BE7B"))
    ws.freeze_panes = "A5"


def build_tariffs(wb):
    ws = wb.create_sheet("Tarifas por região")
    setup(ws, "Tarifas por região", "Use somente preços públicos AWS. O script de atualização preenche as colunas públicas suportadas para sa-east-1 e us-east-1.", 12)
    headers = ["Região AWS", "Moeda", "Outbound AWS Internet\nUS$/GB", "Armazenamento Deep Archive\nUS$/GB-mês", "Recuperação Deep Archive BULK\nUS$/GB", "Recuperação Deep Archive STANDARD\nUS$/GB", "Cópia temporária S3 Standard\nUS$/GB-mês", "S3 PUT/LIST\nUS$/1.000", "S3 GET/TAG\nUS$/1.000", "Job Batch\nUS$/job", "Objeto Batch\nUS$/1.000", "Fonte pública / observações"]
    widths = [16, 11, 15, 20, 23, 27, 26, 17, 17, 15, 20, 58]
    for c, (label, width) in enumerate(zip(headers, widths), 1): ws.cell(4, c, label); header(ws.cell(4, c)); ws.column_dimensions[ws.cell(4, c).column_letter].width = width
    starter = [
        ["sa-east-1", "USD", .15, None, .008, .028, .0405, .007, .00056, .25, .000015, "Valores públicos AWS coletados pelo Raijin em 19/08/2026. Atualize antes de aprovar gastos."],
        ["us-east-1", "USD", .09, None, .0025, .02, .0125, .055, .0004, .25, .001, "Valores públicos de exemplo. Atualize antes de usar."],
    ]
    for r in range(5, 55):
        values = starter[r - 5] if r <= 6 else [None] * 12
        for c, value in enumerate(values, 1): ws.cell(r, c, value); style(ws.cell(r, c), editable=True); rate(ws.cell(r, c)) if 3 <= c <= 11 else None
        ws.row_dimensions[r].height = 27
    ws.auto_filter.ref = "A4:L54"; ws.freeze_panes = "A5"


def build_summary(wb):
    ws = wb.create_sheet("Resumo")
    setup(ws, "Resumo de custo da migração AWS", "Todos os valores são estimativas com preços públicos AWS. Altere a chave ao lado de outbound para FALSE para excluí-lo dos totais.", 8)
    headers = ["Métrica", "Quantidade", "Unidade", "Tarifa usada", "Fonte da tarifa", "Custo estimado", "Cálculo / observação", "Incluir outbound?"]
    widths = [41, 19, 18, 18, 18, 21, 48, 18]
    for c, (label, width) in enumerate(zip(headers, widths), 1): ws.cell(4, c, label); header(ws.cell(4, c)); ws.column_dimensions[ws.cell(4, c).column_letter].width = width
    rows = [
        ("Recuperação Deep Archive BULK", "='Transfer data'!B8*1000*'Transfer data'!B10", "GB / onda", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:L,5,FALSE)", "AWS pública", "=IF('Transfer data'!B11=\"BULK\",IFERROR(B5*D5,\"Não precificado\"),0)", "Somente a parcela em Deep Archive."),
        ("Recuperação Deep Archive STANDARD", "='Transfer data'!B8*1000*'Transfer data'!B10", "GB / onda", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:L,6,FALSE)", "AWS pública", "=IF('Transfer data'!B11=\"STANDARD\",IFERROR(B6*D6,\"Não precificado\"),0)", "Somente a parcela em Deep Archive."),
        ("Cópia temporária restaurada", "='Transfer data'!B8*1000*'Transfer data'!B10*'Transfer data'!B12/(24*30)", "GB-mês / onda", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:L,7,FALSE)", "AWS pública", "=IFERROR(B7*D7,\"Não precificado\")", "Cópia S3 Standard temporária após o restore; a retenção em horas é convertida para mês."),
        ("S3 Batch Operations", "=1", "job / onda", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:L,10,FALSE)", "AWS pública", "=IFERROR(B8*D8,\"Não precificado\")", "Um job de restore por onda arquivada."),
        ("Tarefas de objeto S3 Batch", "=ROUNDUP('Transfer data'!B7/'Transfer data'!B9,0)", "objetos / onda", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:L,11,FALSE)/1000", "AWS pública", "=IFERROR(B9*D9,\"Não precificado\")", "Tarifa de objeto normalizada para uma unidade."),
        ("Discovery e polling de restore", "=(ROUNDUP('Transfer data'!B7/1000,0)*'Transfer data'!B13)+ROUNDUP('Transfer data'!B7/'Transfer data'!B9/1000,0)", "solicitações / onda", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:L,8,FALSE)/1000", "AWS pública", "=IFERROR(B10*D10,\"Não precificado\")", "Estimativa conservadora baseada em ListObjectsV2."),
        ("Leituras de objetos S3", "=ROUNDUP('Transfer data'!B7/'Transfer data'!B9,0)", "solicitações / onda", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:L,9,FALSE)/1000", "AWS pública", "=IFERROR(B11*D11,\"Não precificado\")", "Um GetObject em streaming por arquivo."),
        ("Leituras de tags S3", "=ROUNDUP('Transfer data'!B7/'Transfer data'!B9,0)", "solicitações / onda", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:L,9,FALSE)/1000", "AWS pública", "=IF('Transfer data'!B14,IFERROR(B12*D12,\"Não precificado\"),0)", "Somente se a preservação de tags estiver habilitada."),
        ("Outbound AWS", "='Transfer data'!B8*1000", "GB / onda", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:L,3,FALSE)", "AWS pública", "=IF(H13,IFERROR(B13*D13,\"Não precificado\"),0)", "Use a chave para incluir ou excluir outbound."),
        ("Exclusão antecipada Deep Archive", "='Transfer data'!B15*1000*MAX(0,180-'Transfer data'!B16)/30", "GB-mês / projeto", "=VLOOKUP('Transfer data'!B5,'Tariffs by region'!A:L,4,FALSE)", "AWS pública", "=IF('Transfer data'!B15>0,IFERROR(B14*D14,\"Não precificado\"),0)", "Estimativa da exclusão antecipada para todo o projeto."),
    ]
    for r, values in enumerate(rows, 5):
        for c, value in enumerate(values, 1): ws.cell(r, c, localized_formula(value)); style(ws.cell(r, c))
        ws.cell(r, 8, True if r == 13 else "—"); style(ws.cell(r, 8), editable=r == 13)
        number(ws.cell(r, 2), 4); rate(ws.cell(r, 4)); money(ws.cell(r, 6))
        ws.row_dimensions[r].height = 27
    totals = [("Custo único por wave", "=SUM(F5:F13)"), ("Custo único do projeto", "=F16*'Transfer data'!B9+F14")]
    for r, (label, formula) in enumerate(totals, 16):
        ws.cell(r, 1, label); ws.cell(r, 6, localized_formula(formula))
        for c in range(1, 9): style(ws.cell(r, c))
        ws.cell(r, 1).font = Font(bold=True, color=WHITE); ws.cell(r, 6).font = Font(size=14, bold=True, color=WHITE); ws.cell(r, 6).fill = PatternFill("solid", fgColor=TOTAL); money(ws.cell(r, 6))
        ws.row_dimensions[r].height = 30
    ws.freeze_panes = "A5"
    outbound_toggle = DataValidation(type="list", formula1='"TRUE,FALSE"')
    ws.add_data_validation(outbound_toggle); outbound_toggle.add("H13")


def main():
    wb = Workbook()
    wb.calculation.fullCalcOnLoad = wb.calculation.forceFullCalc = True
    wb.calculation.calcMode = "auto"
    build_transfer_data(wb); build_tariffs(wb); build_summary(wb)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    wb.save(OUTPUT)
    print(OUTPUT)


if __name__ == "__main__": main()
