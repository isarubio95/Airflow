import re
import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple
from dateutil.parser import isoparse
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# ============================================================
# 🧩 EXPRESIONES REGULARES
# ============================================================

RE_PATENT_ID = re.compile(r"^P\d{7}$")                                # ID de patente que comienza con 'P' seguido de 7 dígitos
RE_DATE_PAREN = re.compile(r"^\(\d{2}\.\d{2}\.\d{4}\)$")              # fechas en formato '(DD.MM.YYYY)'
RE_COUNTRY_CODE = re.compile(r"^[A-Z]{2}$")                           # códigos de país de 2 letras en mayúsculas
RE_PDF_URL = re.compile(r"^https://[^ ]+\.pdf$")                      # URLs HTTPS que terminan en '.pdf'
RE_IMAGE_URL = re.compile(r".*\.(pdf|jpg|jpeg|png|gif)$", re.IGNORECASE)  # URLs de imágenes con extensiones comunes (insensible a mayúsculas)
RE_IPC_CODE = re.compile(r"^[A-HY]\d{2}[A-Z]\d{1,4}/\d{2,}$")         # códigos de clasificación IPC
RE_CLASS_DATE = re.compile(r"^\(\d{4}\.\d{2}\)$")                     # fechas de clasificación en formato '(YYYY.MM)'
RE_SPANISH_PROVINCE_CODE = re.compile(r"^(0[1-9]|[1-4]\d|5[0-2])$")   # códigos de provincias españolas (01-52)


# ============================================================
# 🧠 FUNCIONES AUXILIARES
# ============================================================

def get_nested(data, path, default=None):
    """Obtiene un valor anidado de un dict mediante notación de puntos ('a.b.c')."""
    current = data
    for key in path.split('.'):
        if isinstance(current, dict) and key in current:
            current = current[key]
        elif isinstance(current, list) and key.isdigit() and int(key) < len(current):
            current = current[int(key)]
        else:
            return default
    return current


def parse_parenthesis_date(date_str: str):
    """Convierte una fecha tipo '(DD.MM.YYYY)' en objeto datetime."""
    if not date_str or not isinstance(date_str, str) or not RE_DATE_PAREN.match(date_str):
        raise ValueError(f"Formato de fecha inválido: {date_str}")
    cleaned = date_str.strip("() ")
    return datetime.strptime(cleaned, "%d.%m.%Y")


def infer_year_from_id(patent_id: str) -> int:
    """Infiera el año a partir del ID (P9701774 → 1997)."""
    year_digits = int(patent_id[1:3])
    return 1900 + year_digits if year_digits >= 80 else 2000 + year_digits


def is_order_sequential(items, key="orden"):
    """Comprueba si los valores 'orden' son secuenciales del 1 al N."""
    try:
        order_values = sorted(int(x.get(key)) for x in items)
        return order_values == list(range(1, len(items) + 1))
    except Exception:
        return False


def ensure_list(value):
    """Devuelve siempre una lista, incluso si el valor es único o None."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


# ============================================================
# 🧩 VALIDACIÓN DE UNA SOLA PATENTE
# ============================================================

def validate_single_patent(patent: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    """
    Valida una sola patente.
    Devuelve: (es, errores, avisos)
    """
    errors, warnings = [], []

    # ---------- Identificador ----------
    patent_id = get_nested(patent, "_id")
    if not patent_id or not isinstance(patent_id, str) or not RE_PATENT_ID.match(patent_id):
        errors.append(f"_id inválido: {patent_id}")
        return False, errors, warnings

    # ---------- Fechas de creación/modificación ----------
    created_at_str = get_nested(patent, "fechaCrea.$date")
    modified_at_str = get_nested(patent, "fechaModi.$date")
    try:
        if created_at_str:
            created_at = isoparse(created_at_str)
        if modified_at_str:
            modified_at = isoparse(modified_at_str)
        if created_at_str and modified_at_str and modified_at < created_at:
            errors.append("La fecha de modificación es anterior a la de creación.")
    except Exception as e:
        errors.append(f"Error al parsear fechas ISO: {e}")

    # ---------- Referencia ----------
    reference = get_nested(patent, "patente.referencia")
    if not isinstance(reference, dict):
        errors.append("Falta el campo 'patente.referencia'.")
        return False, errors, warnings

    internal_ref = reference.get("refe")
    if internal_ref != patent_id:
        errors.append(f"_id y referencia interna no coinciden ({patent_id} vs {internal_ref}).")

    # ---------- Solicitud ----------
    application_number = get_nested(patent, "patente.referencia.solicitudes.solicitud.0.nsol")
    application_date = get_nested(patent, "patente.referencia.solicitudes.solicitud.0.fsol")

    if not application_number:
        errors.append("Falta el número de solicitud ('nsol').")
    elif application_number != patent_id:
        errors.append(f"El número de solicitud ('nsol') no coincide con el _id ({application_number} vs {patent_id}).")

    if not application_date:
        errors.append("Falta la fecha de solicitud ('fsol').")
    else:
        try:
            parsed_date = parse_parenthesis_date(application_date)
            inferred_year = infer_year_from_id(patent_id)
            if parsed_date.year != inferred_year:
                warnings.append(f"Año de fsol {parsed_date.year} diferente al inferido por _id ({inferred_year}).")
        except Exception as e:
            errors.append(f"Fecha fsol inválida: {e}")

    # ---------- Banderas y contenido ----------
    has_pdfs = reference.get("tienepdf") == "X"
    has_images = reference.get("tieneimg") == "X"
    has_summary = reference.get("tieneresu") == "X"

    pdf_urls = ensure_list(get_nested(patent, "patente.pdfs"))
    image_urls = ensure_list(get_nested(patent, "patente.images"))
    summary_text = get_nested(patent, "patente.resumen")

    if has_pdfs and not pdf_urls:
        errors.append("Marcada como 'tienepdf' pero no hay PDFs.")
    if has_images and not image_urls:
        errors.append("Marcada como 'tieneimg' pero no hay imágenes.")
    if has_summary and (not summary_text or not str(summary_text).strip()):
        errors.append("Marcada como 'tieneresu' pero el resumen está vacío.")

    if not all(isinstance(url, str) and RE_PDF_URL.match(url) for url in pdf_urls):
        errors.append("Algún valor en 'pdfs' no es una URL HTTPS válida que termina en .pdf.")
    if any((not isinstance(url, str)) or (not RE_IMAGE_URL.match(url)) for url in image_urls):
        errors.append("Algún valor en 'images' no parece una imagen válida.")

    # ---------- Inventores ----------
    inventors = ensure_list(get_nested(patent, "patente.referencia.inventores.inventor"))
    if not inventors:
        errors.append("Falta la lista de inventores.")
    else:
        if not is_order_sequential(inventors, "orden"):
            warnings.append("Los inventores no están numerados secuencialmente.")
        for inventor in inventors:
            if not inventor.get("inve") or not isinstance(inventor.get("inve"), str):
                errors.append("Inventor sin nombre válido.")
            country_code = inventor.get("nain")
            if not country_code or not RE_COUNTRY_CODE.match(country_code):
                errors.append(f"Código de país inválido en inventor: {country_code}")

    # ---------- Solicitantes ----------
    applicants = ensure_list(get_nested(patent, "patente.referencia.solicitantes.solicitante"))
    if not applicants:
        errors.append("Falta la lista de solicitantes.")
    else:
        for applicant in applicants:
            if not applicant.get("soli") or not isinstance(applicant.get("soli"), str):
                errors.append("Solicitante sin nombre válido.")
            country = applicant.get("nare")
            if not country or not RE_COUNTRY_CODE.match(country):
                errors.append(f"Código de país inválido en solicitante: {country}")
            if country == "ES":
                province_code = applicant.get("prov")
                if province_code is not None and not RE_SPANISH_PROVINCE_CODE.match(str(province_code)):
                    warnings.append(f"Código de provincia español no estándar: {province_code}")

    # ---------- Clasificaciones ----------
    classifications = ensure_list(get_nested(patent, "patente.referencia.clasificaciones.claint.clai.item"))
    for classification in classifications:
        ipc_code = classification.get("clasificacion")
        if not ipc_code or not RE_IPC_CODE.match(ipc_code):
            warnings.append(f"Clasificación IPC con formato dudoso: {ipc_code}")
        date_field = classification.get("fecha")
        if date_field and not RE_CLASS_DATE.match(date_field):
            warnings.append(f"Formato de fecha IPC no estándar: {date_field}")

    # ---------- Publicaciones ----------
    first_pub = get_nested(patent, "patente.referencia.publicaciones.primera.publicacion")
    other_pubs = ensure_list(get_nested(patent, "patente.referencia.publicaciones.otras.publicacion"))
    try:
        if first_pub and first_pub.get("fpub"):
            first_pub_date = parse_parenthesis_date(first_pub["fpub"])
            for pub in other_pubs:
                if pub.get("fpub"):
                    other_date = parse_parenthesis_date(pub["fpub"])
                    if other_date < first_pub_date:
                        errors.append("Una publicación 'otras' tiene fecha anterior a la 'primera'.")
        first_type = first_pub.get("tipo") if first_pub else None
        if any(pub.get("tipo") == "B1" for pub in other_pubs) and first_type != "A1":
            warnings.append("Hay publicación B1 pero la primera no es A1.")
    except Exception as e:
        errors.append(f"Error alr fechas de publicaciones: {e}")

    def matches_relative_pdf(pub: Dict[str, Any]) -> bool:
        path = pub.get("pdf") or ""
        npub = str(pub.get("npub") or "")
        country = str(pub.get("pais") or "")

        if not path or not npub or not country:
            return False

        parsed = urlparse(path)
        route = parsed.path or path
        segments = [s for s in route.split("/") if s]
        filename = segments[-1] if segments else ""
        name = filename.removesuffix(".pdf")

        # 1) País como segmento exacto
        has_country = country in segments

        # 2) npub aceptado si:
        #    - es un segmento exacto, o
        #    - aparece en el nombre de archivo (en cualquier posición), p. ej. ES-2134734_A1.pdf
        has_npub = (npub in segments) or (npub in name)

        return has_country and has_npub

    if first_pub and not matches_relative_pdf(first_pub):
        warnings.append("Ruta PDF de 'primera' no coherente con país/npub.")
    for pub in other_pubs:
        if not matches_relative_pdf(pub):
            warnings.append("Ruta PDF en 'otras' no coherente con país/npub.")

    # ---------- Año en URLs de imágenes ----------
    inferred_year = infer_year_from_id(patent_id)
    if image_urls and all(isinstance(url, str) for url in image_urls):
        if not all(f"/{inferred_year}/" in url for url in image_urls if url.startswith("https://")):
            warnings.append("Alguna imagen no contiene el año inferido por el _id en la URL.")

    # ---------- Textos mínimos ----------
    title_es = get_nested(patent, "patente.referencia.titu.titu_es")
    if not title_es or not str(title_es).strip():
        errors.append("Título en español vacío.")
    summary = get_nested(patent, "patente.resumen")
    if summary and len(str(summary).strip()) < 10:
        warnings.append("Resumen demasiado corto.")

    return (len(errors) == 0), errors, warnings


# ============================================================
# 📦 VALIDACIÓN DE UN LOTE COMPLETO
# ============================================================

def validate_patent_batch(batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Valida un lote de patentes. 
    También emite logs con errores y avisos.
    """
    valid_patents = []
    total_count = len(batch)
    invalid_count, warning_count = 0, 0

    for patent in batch:
        patent_id = get_nested(patent, "_id") or "ID_DESCONOCIDO"
        is_valid, errors, warnings = validate_single_patent(patent)

        if is_valid:
            valid_patents.append(patent)
            if warnings:
                warning_count += 1
                logger.warning("Patente %s con avisos: %s", patent_id, " | ".join(warnings))
        else:
            invalid_count += 1
            logger.error("Patente %s inválida: %s", patent_id, " | ".join(errors))
            if warnings:
                logger.warning("Patente %s con avisos adicionales: %s", patent_id, " | ".join(warnings))

    logger.info(
        "Lote procesado → Válidas: %d / %d | Inválidas: %d | Con avisos: %d",
        len(valid_patents), total_count, invalid_count, warning_count
    )
    return valid_patents
