"""Pruebas aisladas para la carga PDF de Picking, sin Streamlit ni DB de producción.
Ejecutar: python -m unittest discover -s tests -v
"""
import ast
import io
import re
import unittest
from pathlib import Path

import pandas as pd
import pdfplumber
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4


APP = Path(__file__).resolve().parents[1] / "app.py"
module = ast.parse(APP.read_text(encoding="utf-8"))
target = next(n for n in module.body if isinstance(n, ast.FunctionDef) and n.name == "parse_manifest_pdf")
source = ast.get_source_segment(APP.read_text(encoding="utf-8"), target)


def normalize_sku(value):
    return str(value).strip()


sandbox = {"pd": pd, "pdfplumber": pdfplumber, "re": re,
           "HAS_PDF_LIB": True, "normalize_sku": normalize_sku}
exec(source, sandbox)
parse_manifest_pdf = sandbox["parse_manifest_pdf"]


def generate_pdf(pages):
    f = io.BytesIO()
    c = canvas.Canvas(f, pagesize=A4)
    for lines in pages:
        y = 780
        for line in lines:
            c.drawString(30, y, line)
            y -= 18
        c.showPage()
    c.save()
    return f.getvalue()


class ParsePickingPDFTests(unittest.TestCase):
    def test_sale_then_sku_quantity(self):
        b = generate_pdf([["Venta: 20000123456789", "SKU: 123-ABC", "Cantidad: 2"]])
        df = parse_manifest_pdf(b)
        self.assertEqual(df.iloc[0]["ml_order_id"], "20000123456789")
        self.assertEqual(df.iloc[0]["sku_ml"], "123-ABC")
        self.assertEqual(int(df.iloc[0]["qty"]), 2)

    def test_sku_before_sale(self):
        b = generate_pdf([["Pack ID: 2000099887766 SKU: A-123", "Venta: 20000123456789 Cantidad: 4"]])
        df = parse_manifest_pdf(b)
        self.assertEqual(df.iloc[0]["ml_order_id"], "20000123456789")
        self.assertEqual(int(df.iloc[0]["qty"]), 4)

    def test_two_sales_same_page(self):
        b = generate_pdf([["Venta: 20000123456789 SKU: 111 Cantidad: 1",
                           "Venta: 20000223456789 SKU: 222 Cantidad: 2"]])
        df = parse_manifest_pdf(b)
        self.assertEqual(len(df), 2)
        self.assertEqual(int(df.qty.sum()), 3)

    def test_multi_page(self):
        b = generate_pdf([["Venta: 20000123456789 SKU: 111 Cantidad: 1"],
                          ["Venta: 20000223456789 SKU: 222 Cantidad: 2"]])
        df = parse_manifest_pdf(b)
        self.assertEqual(df.ml_order_id.nunique(), 2)

    def test_multi_sku_same_sale(self):
        b = generate_pdf([["Venta: 20000123456789", "SKU: 111", "Cantidad: 2", "SKU: 222", "Cantidad: 3"]])
        df = parse_manifest_pdf(b)
        self.assertEqual(len(df), 2)
        self.assertEqual(int(df.qty.sum()), 5)

    def test_same_sku_aggregate_quantities(self):
        b = generate_pdf([["Venta: 20000123456789 SKU: 111 Cantidad: 2",
                           "Venta: 20000123456789 SKU: 111 Cantidad: 3"]])
        df = parse_manifest_pdf(b)
        self.assertEqual(len(df), 1)
        self.assertEqual(int(df.iloc[0]["qty"]), 5)

    def test_quantity_before_sku(self):
        b = generate_pdf([["Venta: 20000123456789 Cantidad: 4 SKU: XYZ"]])
        df = parse_manifest_pdf(b)
        self.assertEqual(int(df.iloc[0]["qty"]), 4)

    def test_only_pack_no_sale_is_rejected(self):
        b = generate_pdf([["Pack ID: 20000123456789", "SKU: 111 Cantidad: 2"]])
        with self.assertRaisesRegex(ValueError, "No se encontraron números"):
            parse_manifest_pdf(b)

    def test_missing_qty_is_rejected(self):
        b = generate_pdf([["Venta: 20000123456789 SKU: 111"]])
        with self.assertRaisesRegex(ValueError, "SKU sin cantidad"):
            parse_manifest_pdf(b)

    def test_unpaired_qty_is_rejected(self):
        b = generate_pdf([["Venta: 20000123456789 SKU: 111 Cantidad: 2 Cantidad: 3"]])
        with self.assertRaisesRegex(ValueError, "cantidades sin SKU"):
            parse_manifest_pdf(b)

    def test_order_without_items_blocks_whole_file(self):
        b = generate_pdf([["Venta: 20000123456789 SKU: 111 Cantidad: 2",
                           "Venta: 20000223456789"]])
        with self.assertRaisesRegex(ValueError, "ventas sin productos"):
            parse_manifest_pdf(b)

    def test_sku_before_first_sale_and_quantity(self):
        b = generate_pdf([["SKU: 111 Cantidad: 2 Venta: 20000123456789"]])
        df = parse_manifest_pdf(b)
        self.assertEqual(int(df.iloc[0]["qty"]), 2)

    def test_real_control_two_columns_recovers_buyer(self):
        # Imitación sintética del PDF real de ML, con ambas columnas independientes.
        f = io.BytesIO()
        c = canvas.Canvas(f, pagesize=A4)
        c.drawString(40, 780, "Identificacion")
        c.drawString(300, 780, "Productos")
        c.drawString(40, 740, "48281596778")
        c.drawString(300, 740, "Set ducha Stella")
        c.drawString(40, 720, "Venta: 20000123456789")
        c.drawString(300, 720, "SKU: 110804205005")
        c.drawString(40, 700, "Comprador Ejemplo")
        c.drawString(300, 700, "Cantidad: 1")
        c.save()
        df = parse_manifest_pdf(f.getvalue())
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["buyer"], "Comprador Ejemplo")
        self.assertEqual(df.iloc[0]["sku_ml"], "110804205005")
        self.assertEqual(int(df.iloc[0]["qty"]), 1)

    def test_whole_pdf_rejected_if_second_page_incomplete(self):
        b = generate_pdf([["Venta: 20000123456789 SKU: 111 Cantidad: 2"],
                          ["Venta: 20000223456789 SKU: 222"]])
        with self.assertRaisesRegex(ValueError, "No se creó ninguna OT"):
            parse_manifest_pdf(b)

    def test_pack_id_not_confused_with_sale_quantity(self):
        b = generate_pdf([["Pack ID: 2000015570965807 SKU: 110452603017",
                           "Venta: 2000019028796538 Cantidad: 5"]])
        df = parse_manifest_pdf(b)
        self.assertEqual(df.iloc[0]["ml_order_id"], "2000019028796538")
        self.assertEqual(int(df.iloc[0]["qty"]), 5)

    def test_reject_not_pdf(self):
        with self.assertRaisesRegex(ValueError, "PDF válido"):
            parse_manifest_pdf(b"not a PDF")


if __name__ == "__main__":
    unittest.main()
