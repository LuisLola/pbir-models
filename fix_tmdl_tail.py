from pathlib import Path

path = Path(r'C:\projects\pbir-models\B2B-B2C-SalesOrder\B2B-B2C-SalesOrder.SemanticModel\definition\tables\MedidasCalculadasRetailOnline.tmdl')
text = path.read_text(encoding='utf-8')
start = text.index('measure Entradas_YTD =')
new_tail = '''measure Entradas_YTD =
			CALCULATE(
				SUM(LOL_PBITRAFICOTIENDAS[U_GSP_ENTRADAS]),
				TREATAS(VALUES(Calendario[Date]), LOL_PBITRAFICOTIENDAS[U_GSP_DATE])
			)
	formatString: #,0
	lineageTag: c1e8e6b1-44a2-4f58-8e77-0a1c4d2b3f7a

	measure Entradas_YTD_LY =
			CALCULATE(
				[Entradas_YTD],
				SAMEPERIODLASTYEAR(Calendario[Date])
			)
	formatString: #,0
	lineageTag: b2f0d8c2-55b3-4f69-9f88-1b2d5e3c4a8b

	measure Evolucion_Entradas_YTD_Porcentaje =
			DIVIDE(
				[Entradas_YTD] - [Entradas_YTD_LY],
				[Entradas_YTD_LY]
			)
	formatString: 0.00%;-0.00%;0.00%
	lineageTag: d3f1e9d3-66c4-4070-af99-2c3e6f4d5b9c

	measure Evolucion_Exterior_YTD_Porcentaje =
			VAR FechasActual =
				DATESYTD(Calendario[Date])
			VAR FechasAnterior =
				DATESYTD(DATEADD(Calendario[Date], -1, YEAR))
			VAR Actual =
				CALCULATE(
					SUM(LOL_PBITRAFICOTIENDAS[U_GSP_EXTERIOR]),
					TREATAS(FechasActual, LOL_PBITRAFICOTIENDAS[U_GSP_DATE])
				)
			VAR Anterior =
				CALCULATE(
					SUM(LOL_PBITRAFICOTIENDAS[U_GSP_EXTERIOR]),
					TREATAS(FechasAnterior, LOL_PBITRAFICOTIENDAS[U_GSP_DATE])
				)
			RETURN
				DIVIDE(Actual - Anterior, Anterior)
	annotation PBI_FormatHint = {"isGeneralNumber":true}
	lineageTag: e4f2fab4-77d5-47e1-9b00-3d4f7e5f6a0b

	measure Exterior_YTD =
			CALCULATE(
				SUM(LOL_PBITRAFICOTIENDAS[U_GSP_EXTERIOR]),
				DATESYTD(Calendario[Date]),
				TREATAS(VALUES(Calendario[Date]), LOL_PBITRAFICOTIENDAS[U_GSP_DATE])
			)
	formatString: 0
	annotation PBI_FormatHint = {"isDecimal":true}
	lineageTag: f5a3b8c5-88e6-4a92-bc11-4e5f8d6a7b1c

	measure Exterior_YTD_LY =
			VAR FechaFin = MAX(Calendario[Date])
			VAR FechaIni = DATE(YEAR(FechaFin), 1, 1)
			VAR FechaIniLY = DATE(YEAR(FechaIni) - 1, 1, 1)
			VAR FechaFinLY = DATE(YEAR(FechaFin) - 1, MONTH(FechaFin), DAY(FechaFin))
			RETURN
				CALCULATE(
					SUM(LOL_PBITRAFICOTIENDAS[U_GSP_EXTERIOR]),
					FILTER(
						KEEPFILTERS(LOL_PBITRAFICOTIENDAS),
						LOL_PBITRAFICOTIENDAS[U_GSP_DATE] >= FechaIniLY &&
						LOL_PBITRAFICOTIENDAS[U_GSP_DATE] <= FechaFinLY
					)
				)
	formatString: 0
	lineageTag: 06d4e6c6-99f7-4d03-d122-5f6a9b7c8d2e

	column Column
		formatString: 0
		lineageTag: 32f6e1b2-46e5-4fc5-9322-68559e9d8259
		summarizeBy: sum
		isNameInferred
		sourceColumn: [Column]

		annotation SummarizationSetBy = Automatic

	partition MedidasCalculadasRetailOnline = calculated
'''
text = text[:start] + new_tail
path.write_text(text, encoding='utf-8')
print('ok')
