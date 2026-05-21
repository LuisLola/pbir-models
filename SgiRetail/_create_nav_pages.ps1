# Generate Level 2 (channel detail) and Level 3 (store detail) mobile pages
# All string literals are ASCII-safe; special chars use JSON unicode escapes (\uXXXX)

$enc = New-Object System.Text.UTF8Encoding $false
$schemaVis = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.8.0/schema.json"
$schemaMob = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainerMobileState/2.3.0/schema.json"
$schemaPage = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json"
$dot = [char]0x00B7  # middle dot U+00B7, safe to use in double-quoted strings at runtime

function wf($path, $content) {
    $dir = Split-Path $path
    if (-not (Test-Path $dir)) { New-Item -Force -ItemType Directory $dir | Out-Null }
    [System.IO.File]::WriteAllText($path, $content, $enc)
}

$pagesBase = "c:\projects\pbir-models\SgiRetail\SgiRetail.Report\definition\pages"

function shapeVis($name, $x, $y, $z, $h, $w, $color) {
    $json = '{' + "`n"
    $json += '  "$schema": "' + $schemaVis + '",' + "`n"
    $json += '  "name": "' + $name + '",' + "`n"
    $json += '  "position": { "x": ' + $x + ', "y": ' + $y + ', "z": ' + $z + ', "height": ' + $h + ', "width": ' + $w + ', "tabOrder": ' + $z + ' },' + "`n"
    $json += '  "visual": { "visualType": "shape", "objects": {' + "`n"
    $json += '    "general": [{"properties": {"shapeType": {"expr": {"Literal": {"Value": "0D"}}}}}],' + "`n"
    $json += '    "fill": [{"properties": {"fillColor": {"solid": {"color": {"expr": {"Literal": {"Value": "' + "'" + $color + "'" + '"}}}}}}}],' + "`n"
    $json += '    "line": [{"properties": {"strokeWidth": {"expr": {"Literal": {"Value": "0D"}}}}}]' + "`n"
    $json += '  } }' + "`n"
    $json += '}'
    return $json
}

function mobJson($x, $y, $z, $h, $w) {
    $json = '{' + "`n"
    $json += '  "$schema": "' + $schemaMob + '",' + "`n"
    $json += '  "position": { "x": ' + $x + ', "y": ' + $y + ', "z": ' + $z + ', "height": ' + $h + ', "width": ' + $w + ', "tabOrder": ' + $z + ' }' + "`n"
    $json += '}'
    return $json
}

function oneTxtVis($name, $x, $y, $z, $h, $w, $txt, $size, $color, $bold) {
    $boldStr = if ($bold) { ', "fontWeight": "bold"' } else { '' }
    $json = '{' + "`n"
    $json += '  "$schema": "' + $schemaVis + '",' + "`n"
    $json += '  "name": "' + $name + '",' + "`n"
    $json += '  "position": { "x": ' + $x + ', "y": ' + $y + ', "z": ' + $z + ', "height": ' + $h + ', "width": ' + $w + ', "tabOrder": ' + $z + ' },' + "`n"
    $json += '  "visual": { "visualType": "textbox", "objects": { "general": [{ "properties": { "paragraphs": [' + "`n"
    $json += '    { "horizontalTextAlignment": "center", "textRuns": [{"value": "' + $txt + '", "textStyle": {"fontSize": "' + $size + '", "color": "' + $color + '"' + $boldStr + '}}] }' + "`n"
    $json += '  ] } }] } }' + "`n"
    $json += '}'
    return $json
}

function twoTxtVis($name, $x, $y, $z, $h, $w, $t1, $s1, $c1, $b1, $t2, $s2, $c2, $b2) {
    $boldStr1 = if ($b1) { ', "fontWeight": "bold"' } else { '' }
    $boldStr2 = if ($b2) { ', "fontWeight": "bold"' } else { '' }
    $json = '{' + "`n"
    $json += '  "$schema": "' + $schemaVis + '",' + "`n"
    $json += '  "name": "' + $name + '",' + "`n"
    $json += '  "position": { "x": ' + $x + ', "y": ' + $y + ', "z": ' + $z + ', "height": ' + $h + ', "width": ' + $w + ', "tabOrder": ' + $z + ' },' + "`n"
    $json += '  "visual": { "visualType": "textbox", "objects": { "general": [{ "properties": { "paragraphs": [' + "`n"
    $json += '    { "horizontalTextAlignment": "center", "textRuns": [{"value": "' + $t1 + '", "textStyle": {"fontSize": "' + $s1 + '", "color": "' + $c1 + '"' + $boldStr1 + '}}] },' + "`n"
    $json += '    { "horizontalTextAlignment": "center", "textRuns": [{"value": "' + $t2 + '", "textStyle": {"fontSize": "' + $s2 + '", "color": "' + $c2 + '"' + $boldStr2 + '}}] }' + "`n"
    $json += '  ] } }] } }' + "`n"
    $json += '}'
    return $json
}

function cardVis($name, $x, $y, $z, $h, $w, $entity, $prop, $dispName, $calloutSize, $calloutColor, $showLabel, $labelText, $labelColor, $labelSize) {
    $calloutColorBlock = ''
    if ($calloutColor) {
        $calloutColorBlock = ', "fontColor": {"solid": {"color": {"expr": {"Literal": {"Value": "' + "'" + $calloutColor + "'" + '"}}}}}'
    }
    $labelShowVal = if ($showLabel) { 'true' } else { 'false' }
    $labelBlock = ''
    if ($showLabel) {
        $labelBlock = ', "text": {"expr": {"Literal": {"Value": "' + "'" + $labelText + "'"  + '"}}}, "fontColor": {"solid": {"color": {"expr": {"Literal": {"Value": "' + "'" + $labelColor + "'" + '"}}}}}'  + ', "fontSize": {"expr": {"Literal": {"Value": "' + $labelSize + 'D"}}}'
    }
    $json = '{' + "`n"
    $json += '  "$schema": "' + $schemaVis + '",' + "`n"
    $json += '  "name": "' + $name + '",' + "`n"
    $json += '  "position": { "x": ' + $x + ', "y": ' + $y + ', "z": ' + $z + ', "height": ' + $h + ', "width": ' + $w + ', "tabOrder": ' + $z + ' },' + "`n"
    $json += '  "visual": { "visualType": "cardVisual",' + "`n"
    $json += '    "query": { "queryState": { "Data": { "projections": [' + "`n"
    $json += '      { "field": { "Measure": { "Expression": { "SourceRef": { "Entity": "' + $entity + '" } }, "Property": "' + $prop + '" } },' + "`n"
    $json += '        "queryRef": "' + $entity + '.' + $prop + '", "nativeQueryRef": "' + $prop + '", "displayName": "' + $dispName + '" }' + "`n"
    $json += '    ] } } },' + "`n"
    $json += '    "objects": {' + "`n"
    $json += '      "label": [{"properties": { "show": {"expr": {"Literal": {"Value": "' + $labelShowVal + '"}}}' + $labelBlock + ' }}],' + "`n"
    $json += '      "border": [{"properties": {"show": {"expr": {"Literal": {"Value": "false"}}}}}],' + "`n"
    $json += '      "divider": [{"properties": {"show": {"expr": {"Literal": {"Value": "false"}}}}}],' + "`n"
    $json += '      "accentBar": [{"properties": {"show": {"expr": {"Literal": {"Value": "false"}}}}}],' + "`n"
    $json += '      "callout": [{"properties": {"fontSize": {"expr": {"Literal": {"Value": "' + $calloutSize + 'D"}}}' + $calloutColorBlock + ' }}]' + "`n"
    $json += '    }' + "`n"
    $json += '  }' + "`n"
    $json += '}'
    return $json
}

function tblVis($name, $x, $y, $z, $h, $w) {
    $json = '{' + "`n"
    $json += '  "$schema": "' + $schemaVis + '",' + "`n"
    $json += '  "name": "' + $name + '",' + "`n"
    $json += '  "position": { "x": ' + $x + ', "y": ' + $y + ', "z": ' + $z + ', "height": ' + $h + ', "width": ' + $w + ', "tabOrder": ' + $z + ' },' + "`n"
    $json += '  "visual": { "visualType": "tableEx",' + "`n"
    $json += '    "query": { "queryState": { "Values": { "projections": [' + "`n"
    $json += '      { "field": { "Column": { "Expression": {"SourceRef": {"Entity": "LOL_PBITIENDAS"}}, "Property": "NombreTienda" } },' + "`n"
    $json += '        "queryRef": "LOL_PBITIENDAS.NombreTienda", "nativeQueryRef": "NombreTienda" },' + "`n"
    $json += '      { "field": { "Measure": { "Expression": {"SourceRef": {"Entity": "MedidasRetail"}}, "Property": "Ventas_YTD" } },' + "`n"
    $json += '        "queryRef": "MedidasRetail.Ventas_YTD", "nativeQueryRef": "Ventas_YTD", "displayName": "Ventas YTD" },' + "`n"
    $json += '      { "field": { "Measure": { "Expression": {"SourceRef": {"Entity": "MedidasCanales"}}, "Property": "CumplimientoObjetivo_%_Tienda" } },' + "`n"
    $json += '        "queryRef": "MedidasCanales.CumplimientoObjetivo_%_Tienda", "nativeQueryRef": "CumplimientoObjetivo_%_Tienda", "displayName": "% Obj" },' + "`n"
    $json += '      { "field": { "Measure": { "Expression": {"SourceRef": {"Entity": "MedidasCanales"}}, "Property": "Ventas_YTD_Var_%_Tienda" } },' + "`n"
    $json += '        "queryRef": "MedidasCanales.Ventas_YTD_Var_%_Tienda", "nativeQueryRef": "Ventas_YTD_Var_%_Tienda", "displayName": "vs PY" }' + "`n"
    $json += '    ] } }, "filters": [' + "`n"
    $json += '      { "filter": { "entity": "LOL_PBITIENDAS", "condition": { "In": {' + "`n"
    $json += '        "expressions": [{"Column": {"Expression": {"SourceRef": {"Entity": "LOL_PBITIENDAS"}}, "Property": "TipoTienda"}}],' + "`n"
    $json += '        "values": [[{"Literal": {"Value": "' + "'TIENDA'" + '"}}]]' + "`n"
    $json += '      } } } }' + "`n"
    $json += '    ] },' + "`n"
    $json += '    "drillFilterOtherVisuals": true' + "`n"
    $json += '  }' + "`n"
    $json += '}'
    return $json
}

function slcVis($name, $x, $y, $z, $h, $w, $entity, $prop) {
    $json = '{' + "`n"
    $json += '  "$schema": "' + $schemaVis + '",' + "`n"
    $json += '  "name": "' + $name + '",' + "`n"
    $json += '  "position": { "x": ' + $x + ', "y": ' + $y + ', "z": ' + $z + ', "height": ' + $h + ', "width": ' + $w + ', "tabOrder": ' + $z + ' },' + "`n"
    $json += '  "visual": { "visualType": "slicer",' + "`n"
    $json += '    "query": { "queryState": { "Values": { "projections": [' + "`n"
    $json += '      { "field": { "Column": { "Expression": {"SourceRef": {"Entity": "' + $entity + '"}}, "Property": "' + $prop + '" } },' + "`n"
    $json += '        "queryRef": "' + $entity + '.' + $prop + '", "nativeQueryRef": "' + $prop + '", "active": true }' + "`n"
    $json += '    ] } } },' + "`n"
    $json += '    "objects": {"data": [{"properties": {}}]}' + "`n"
    $json += '  }' + "`n"
    $json += '}'
    return $json
}

# ================================================================
# LEVEL 2 -- CHANNEL DETAIL PAGES
# ================================================================
# Note: middle dot separator is written as $dot (JSON unicode escape)

$channels = @(
    @{
        id = "f1e2d3c4b5a6f7e8d9c0"; displayName = "Canal Tienda"; channelName = "TIENDA"
        heroEyebrow = "VENTA NETA  $dot  TIENDA"
        ventasEnt = "MedidasCanales"; ventasProp = "Ventas_YTD_Tienda"
        progEnt = "MedidasCanales"; progProp = "Texto_Progreso_Tienda"
        objEnt = "MedidasCanales"; objProp = "Pilla_VsOBJ_Tienda"
        pyEnt = "MedidasCanales"; pyProp = "Pilla_VsPY_Tienda"
        tmEnt = "MedidasCanales"; tmProp = "PromedioVentaTransaccion_YTD_Tienda"
        uptEnt = "MedidasCanales"; uptProp = "UPT_YTD_Tienda"
        convEnt = "MedidasCanales"; convProp = "Conversion_YTD_Tienda"
        atracEnt = "MedidasCanales"; atracProp = "Atraccion_YTD_Tienda"; hasAtrac = $true
        sectionLabel = "TIENDAS"; hasTable = $true
        noDesgloseText = ""
        pageHeight = 960; footerY = 926
    },
    @{
        id = "a1b2c3d4e5f6a7b8c9da"; displayName = "Canal ECI"; channelName = "CORNER ECI"
        heroEyebrow = "VENTA NETA  $dot  CORNER ECI"
        ventasEnt = "MedidasCanales"; ventasProp = "Ventas_YTD_ECI"
        progEnt = "MedidasCanales"; progProp = "Texto_Progreso_ECI"
        objEnt = "MedidasCanales"; objProp = "Pilla_VsOBJ_ECI"
        pyEnt = "MedidasCanales"; pyProp = "Pilla_VsPY_ECI"
        tmEnt = "MedidasCanales"; tmProp = "PromedioVentaTransaccion_YTD_ECI"
        uptEnt = "MedidasCanales"; uptProp = "UPT_YTD_ECI"
        convEnt = "MedidasCanales"; convProp = "Conversion_YTD_ECI"
        atracEnt = ""; atracProp = ""; hasAtrac = $false
        sectionLabel = "SIN DESGLOSE"
        noDesgloseText = "Este canal no tiene desglose por corners en este periodo."
        pageHeight = 480; footerY = 450
    },
    @{
        id = "b2c3d4e5f6a7b8c9d0eb"; displayName = "Canal Online"; channelName = "ONLINE"
        heroEyebrow = "VENTA NETA  $dot  ONLINE"
        ventasEnt = "MedidasCanales"; ventasProp = "Ventas_YTD_Online"
        progEnt = "MedidasCanales"; progProp = "Texto_Progreso_Online"
        objEnt = "MedidasCanales"; objProp = "Pilla_VsOBJ_Online"
        pyEnt = "MedidasCanales"; pyProp = "Pilla_VsPY_Online"
        tmEnt = "MedidasCanales"; tmProp = "PromedioVentaTransaccion_YTD_Online"
        uptEnt = "MedidasCanales"; uptProp = "UPT_YTD_Online"
        convEnt = "MedidasCanales"; convProp = "Conversion_YTD_Online"
        atracEnt = ""; atracProp = ""; hasAtrac = $false
        sectionLabel = "SIN DESGLOSE"
        noDesgloseText = "Este canal no tiene desglose por fuentes en este periodo."
        pageHeight = 480; footerY = 450
    },
    @{
        id = "c3d4e5f6a7b8c9d0e1fc"; displayName = "Canal Marketplaces"; channelName = "MARKETPLACES"
        heroEyebrow = "VENTA NETA  $dot  MARKETPLACES"
        ventasEnt = "MedidasCanales"; ventasProp = "Ventas_YTD_Marketplace"
        progEnt = "MedidasCanales"; progProp = "Texto_Progreso_Marketplace"
        objEnt = "MedidasCanales"; objProp = "Pilla_VsOBJ_Marketplace"
        pyEnt = "MedidasCanales"; pyProp = "Pilla_VsPY_Marketplace"
        tmEnt = "MedidasCanales"; tmProp = "PromedioVentaTransaccion_YTD_Marketplace"
        uptEnt = "MedidasCanales"; uptProp = "UPT_YTD_Marketplace"
        convEnt = "MedidasCanales"; convProp = "Conversion_YTD_Marketplace"
        atracEnt = ""; atracProp = ""; hasAtrac = $false
        sectionLabel = "SIN DESGLOSE"
        noDesgloseText = "Este canal no tiene desglose por marketplaces en este periodo."
        pageHeight = 480; footerY = 450
    }
)

foreach ($ch in $channels) {
    $pageId = $ch.id
    $pageDir = "$pagesBase\$pageId"
    $visDir = "$pageDir\visuals"

    $pageJson = '{ "$schema": "' + $schemaPage + '", "name": "' + $pageId + '", "displayName": "' + $ch.displayName + '", "displayOption": "ActualSize", "height": ' + $ch.pageHeight + ', "width": 360 }'
    wf "$pageDir\page.json" $pageJson

    # Backgrounds
    wf "$visDir\bg003\visual.json" (shapeVis "bg003" 0 0 100 $ch.pageHeight 360 "#F7F6F2")
    wf "$visDir\bg003\mobile.json" (mobJson 0 0 100 $ch.pageHeight 324)
    wf "$visDir\bg001\visual.json" (shapeVis "bg001" 0 0 1000 56 360 "#E11A6F")
    wf "$visDir\bg001\mobile.json" (mobJson 0 0 1000 56 324)
    wf "$visDir\bg002\visual.json" (shapeVis "bg002" 0 92 2000 148 360 "#10182F")
    wf "$visDir\bg002\mobile.json" (mobJson 0 92 2000 148 324)
    wf "$visDir\bg004\visual.json" (shapeVis "bg004" 0 240 6900 50 360 "#FFFFFF")
    wf "$visDir\bg004\mobile.json" (mobJson 0 240 6900 50 324)

    # Header: 2-line textbox (eyebrow + channel name)
    wf "$visDir\txt001\visual.json" (twoTxtVis "txt001" 0 0 3001 56 360 "LOLA CASADEMUNT $dot CANAL" "8pt" "#FFFFFF" $false $ch.channelName "14pt" "#FFFFFF" $true)
    wf "$visDir\txt001\mobile.json" (mobJson 0 0 3001 56 324)

    # Period chip
    wf "$visDir\crd001\visual.json" (cardVis "crd001" 14 56 4000 36 332 "MedidasCalculadasRetailOnline" "RangoPeriodoSeleccionado" "Periodo" 11 "#F7F6F2" $false "" "" 0)
    wf "$visDir\crd001\mobile.json" (mobJson 14 56 4000 36 296)

    # Hero
    wf "$visDir\txt003\visual.json" (oneTxtVis "txt003" 14 98 5100 16 332 $ch.heroEyebrow "8pt" "#B0B7CC" $true)
    wf "$visDir\txt003\mobile.json" (mobJson 14 98 5100 16 296)
    wf "$visDir\crd002\visual.json" (cardVis "crd002" 14 112 9000 50 200 $ch.ventasEnt $ch.ventasProp "Ventas Canal" 22 "#FFFFFF" $false "" "" 0)
    wf "$visDir\crd002\mobile.json" (mobJson 14 112 9000 50 200)
    wf "$visDir\txt004\visual.json" (cardVis "txt004" 14 164 5200 18 332 $ch.progEnt $ch.progProp "Progreso" 9 "#B0B7CC" $false "" "" 0)
    wf "$visDir\txt004\mobile.json" (mobJson 14 164 5200 18 296)
    wf "$visDir\pill001\visual.json" (cardVis "pill001" 16 186 5300 26 148 $ch.objEnt $ch.objProp "vs OBJ" 11 "" $false "" "" 0)
    wf "$visDir\pill001\mobile.json" (mobJson 14 186 5300 26 142)
    wf "$visDir\pill002\visual.json" (cardVis "pill002" 170 186 5400 26 148 $ch.pyEnt $ch.pyProp "vs PY" 11 "" $false "" "" 0)
    wf "$visDir\pill002\mobile.json" (mobJson 164 186 5400 26 142)

    # 4-col KPI strip
    wf "$visDir\crd003\visual.json" (cardVis "crd003" 0 240 7011 50 90 $ch.tmEnt $ch.tmProp "Ticket M." 10 "" $true "Ticket M." "#8089A2" 7)
    wf "$visDir\crd003\mobile.json" (mobJson 0 240 7011 50 81)
    wf "$visDir\crd004\visual.json" (cardVis "crd004" 90 240 7012 50 90 $ch.uptEnt $ch.uptProp "UPT" 10 "" $true "UPT" "#8089A2" 7)
    wf "$visDir\crd004\mobile.json" (mobJson 81 240 7012 50 81)
    wf "$visDir\crd005\visual.json" (cardVis "crd005" 180 240 7013 50 90 $ch.convEnt $ch.convProp "Conv." 10 "" $true "Conv." "#8089A2" 7)
    wf "$visDir\crd005\mobile.json" (mobJson 162 240 7013 50 81)

    if ($ch.hasAtrac) {
        wf "$visDir\crd006\visual.json" (cardVis "crd006" 270 240 7014 50 90 $ch.atracEnt $ch.atracProp "Atrac." 10 "" $true "Atrac." "#8089A2" 7)
        wf "$visDir\crd006\mobile.json" (mobJson 243 240 7014 50 81)
    } else {
        wf "$visDir\crd006\visual.json" (twoTxtVis "crd006" 270 240 7014 50 90 "ATRAC." "7pt" "#8089A2" $true "N/D" "13pt" "#0A0F1E" $true)
        wf "$visDir\crd006\mobile.json" (mobJson 243 240 7014 50 81)
    }

    # Section label
    wf "$visDir\txt005\visual.json" (oneTxtVis "txt005" 14 290 8000 28 332 $ch.sectionLabel "9pt" "#8089A2" $true)
    wf "$visDir\txt005\mobile.json" (mobJson 14 290 8000 28 296)

    # Content
    if ($ch.hasTable) {
        wf "$visDir\tbl001\visual.json" (tblVis "tbl001" 0 318 8100 590 360)
        wf "$visDir\tbl001\mobile.json" (mobJson 0 318 8100 590 324)
    } else {
        wf "$visDir\bg005\visual.json" (shapeVis "bg005" 14 318 8100 120 332 "#F7F6F2")
        wf "$visDir\bg005\mobile.json" (mobJson 14 318 8100 120 296)
        wf "$visDir\txt006\visual.json" (twoTxtVis "txt006" 14 318 8200 120 332 "SIN DESGLOSE" "9pt" "#8089A2" $true $ch.noDesgloseText "11pt" "#3A4055" $false)
        wf "$visDir\txt006\mobile.json" (mobJson 14 318 8200 120 296)
    }

    # Footer
    wf "$visDir\txt007\visual.json" (oneTxtVis "txt007" 0 $ch.footerY 9900 20 360 "ACTUALIZADO  $dot  LOLA CASADEMUNT" "8pt" "#8089A2" $false)
    wf "$visDir\txt007\mobile.json" (mobJson 0 $ch.footerY 9900 20 324)

    Write-Host "Created: $($ch.displayName)"
}

# ================================================================
# LEVEL 3 -- STORE DETAIL PAGE
# ================================================================

$storePageId = "d4e5f6a7b8c9d0e1f2ad"
$spDir = "$pagesBase\$storePageId"
$svDir = "$spDir\visuals"

$spJson = '{ "$schema": "' + $schemaPage + '", "name": "' + $storePageId + '", "displayName": "Detalle Tienda", "displayOption": "ActualSize", "height": 750, "width": 360 }'
wf "$spDir\page.json" $spJson

# Backgrounds
wf "$svDir\bg003\visual.json" (shapeVis "bg003" 0 0 100 750 360 "#F7F6F2")
wf "$svDir\bg003\mobile.json" (mobJson 0 0 100 750 324)
wf "$svDir\bg001\visual.json" (shapeVis "bg001" 0 0 1000 56 360 "#E11A6F")
wf "$svDir\bg001\mobile.json" (mobJson 0 0 1000 56 324)
wf "$svDir\bg002\visual.json" (shapeVis "bg002" 0 132 2000 148 360 "#10182F")
wf "$svDir\bg002\mobile.json" (mobJson 0 132 2000 148 324)
wf "$svDir\bg004\visual.json" (shapeVis "bg004" 0 280 6900 100 360 "#FFFFFF")
wf "$svDir\bg004\mobile.json" (mobJson 0 280 6900 100 324)
wf "$svDir\bg005\visual.json" (shapeVis "bg005" 0 404 8100 130 360 "#FFFFFF")
wf "$svDir\bg005\mobile.json" (mobJson 0 404 8100 130 324)
wf "$svDir\bg006\visual.json" (shapeVis "bg006" 0 554 9100 110 360 "#FFFFFF")
wf "$svDir\bg006\mobile.json" (mobJson 0 554 9100 110 324)

# Header
wf "$svDir\txt001\visual.json" (oneTxtVis "txt001" 0 4 3001 22 360 "TIENDA  $dot  DETALLE" "8pt" "#FFFFFF" $false)
wf "$svDir\txt001\mobile.json" (mobJson 0 4 3001 22 324)
wf "$svDir\crd001\visual.json" (cardVis "crd001" 14 26 3002 30 332 "MedidasIndicadores" "NombreTienda_Selected" "Tienda" 11 "#FFFFFF" $false "" "" 0)
wf "$svDir\crd001\mobile.json" (mobJson 14 26 3002 30 296)

# Slicer
wf "$svDir\slc001\visual.json" (slcVis "slc001" 14 56 3500 36 332 "LOL_PBITIENDAS" "NombreTienda")
wf "$svDir\slc001\mobile.json" (mobJson 14 56 3500 36 296)

# Period chip
wf "$svDir\crd002\visual.json" (cardVis "crd002" 14 96 4000 36 332 "MedidasCalculadasRetailOnline" "RangoPeriodoSeleccionado" "Periodo" 11 "#F7F6F2" $false "" "" 0)
wf "$svDir\crd002\mobile.json" (mobJson 14 96 4000 36 296)

# Hero
wf "$svDir\txt003\visual.json" (oneTxtVis "txt003" 14 138 5100 16 332 "VENTA NETA" "8pt" "#B0B7CC" $true)
wf "$svDir\txt003\mobile.json" (mobJson 14 138 5100 16 296)
wf "$svDir\crd003\visual.json" (cardVis "crd003" 14 152 9000 50 200 "MedidasRetail" "Ventas_YTD" "Ventas" 22 "#FFFFFF" $false "" "" 0)
wf "$svDir\crd003\mobile.json" (mobJson 14 152 9000 50 200)
wf "$svDir\txt004\visual.json" (cardVis "txt004" 14 204 5200 18 332 "MedidasIndicadores" "Texto_Progreso_Total" "Progreso" 9 "#B0B7CC" $false "" "" 0)
wf "$svDir\txt004\mobile.json" (mobJson 14 204 5200 18 296)
wf "$svDir\pill001\visual.json" (cardVis "pill001" 16 226 5300 26 148 "MedidasIndicadores" "Pilla_VsOBJ_Total" "vs OBJ" 11 "" $false "" "" 0)
wf "$svDir\pill001\mobile.json" (mobJson 14 226 5300 26 142)
wf "$svDir\pill002\visual.json" (cardVis "pill002" 170 226 5400 26 148 "MedidasIndicadores" "Pilla_VsPY_Total" "vs PY" 11 "" $false "" "" 0)
wf "$svDir\pill002\mobile.json" (mobJson 164 226 5400 26 142)

# 2x2 KPI grid
wf "$svDir\crd004\visual.json" (cardVis "crd004" 0 280 7001 50 180 "MedidasRetail" "PromedioVentaTransaccion_YTD" "Ticket Medio" 13 "" $true "Ticket Medio" "#8089A2" 9)
wf "$svDir\crd004\mobile.json" (mobJson 0 280 7001 50 162)
wf "$svDir\crd005\visual.json" (cardVis "crd005" 180 280 7002 50 180 "MedidasRetail" "UPT_YTD" "UPT" 13 "" $true "UPT" "#8089A2" 9)
wf "$svDir\crd005\mobile.json" (mobJson 162 280 7002 50 162)
wf "$svDir\crd006\visual.json" (cardVis "crd006" 0 330 7003 50 180 "MedidasTrafico" "Tasa_Conversion_YTD" "% Conversion" 13 "" $true "% Conversion" "#8089A2" 9)
wf "$svDir\crd006\mobile.json" (mobJson 0 330 7003 50 162)
wf "$svDir\crd007\visual.json" (cardVis "crd007" 180 330 7004 50 180 "MedidasTrafico" "Tasa_Atraccion_YTD" "% Atraccion" 13 "" $true "% Atraccion" "#8089A2" 9)
wf "$svDir\crd007\mobile.json" (mobJson 162 330 7004 50 162)

# Evolution section
wf "$svDir\txt005\visual.json" (oneTxtVis "txt005" 14 380 8000 24 332 "EVOLUCION  $dot  PERIODO" "9pt" "#8089A2" $true)
wf "$svDir\txt005\mobile.json" (mobJson 14 380 8000 24 296)
wf "$svDir\txt006\visual.json" (twoTxtVis "txt006" 14 404 8200 130 332 "Evolucion diaria" "10pt" "#8089A2" $false "Disponible en vista escritorio." "10pt" "#8089A2" $false)
wf "$svDir\txt006\mobile.json" (mobJson 14 404 8200 130 296)

# Comparativa section
wf "$svDir\txt007\visual.json" (oneTxtVis "txt007" 14 534 9000 24 332 "COMPARATIVA" "9pt" "#8089A2" $true)
wf "$svDir\txt007\mobile.json" (mobJson 14 534 9000 24 296)
wf "$svDir\crd008\visual.json" (cardVis "crd008" 14 554 9200 50 200 "MedidasRetail" "Ventas_YTD" "Periodo actual" 18 "" $true "Periodo actual" "#8089A2" 9)
wf "$svDir\crd008\mobile.json" (mobJson 14 554 9200 50 200)
wf "$svDir\crd009\visual.json" (cardVis "crd009" 14 604 9300 50 200 "MedidasRetail" "Ventas_YTD_LY" "Mismo periodo 2024" 18 "#8089A2" $true "Mismo periodo 2024" "#8089A2" 9)
wf "$svDir\crd009\mobile.json" (mobJson 14 604 9300 50 200)

# Footer
wf "$svDir\txt008\visual.json" (oneTxtVis "txt008" 0 710 9900 20 360 "ACTUALIZADO  $dot  LOLA CASADEMUNT" "8pt" "#8089A2" $false)
wf "$svDir\txt008\mobile.json" (mobJson 0 710 9900 20 324)

Write-Host "Level 3 store detail page created."

# ================================================================
# UPDATE pages.json
# ================================================================

$pagesJson = "$pagesBase\pages.json"
$newOrder = @(
    "a9f3b2e1d4c8f7a5b6e3",
    "fb03eec38049b231e4b7",
    "09576d5010690863b17b",
    "e5f6a7b8c9d0e1f2a3b4",
    "f1e2d3c4b5a6f7e8d9c0",
    "a1b2c3d4e5f6a7b8c9da",
    "b2c3d4e5f6a7b8c9d0eb",
    "c3d4e5f6a7b8c9d0e1fc",
    "d4e5f6a7b8c9d0e1f2ad"
)

$orderLines = ($newOrder | ForEach-Object { '    "' + $_ + '"' }) -join ",`n"
$newPagesContent = '{' + "`n"
$newPagesContent += '  "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",' + "`n"
$newPagesContent += '  "pageOrder": [' + "`n"
$newPagesContent += $orderLines + "`n"
$newPagesContent += '  ],' + "`n"
$newPagesContent += '  "activePageName": "e5f6a7b8c9d0e1f2a3b4"' + "`n"
$newPagesContent += '}'
wf $pagesJson $newPagesContent

Write-Host "pages.json updated."
Write-Host "All done!"
