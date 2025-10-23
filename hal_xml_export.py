import io
import re
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st

# ==============================
# Utilitaires internes
# ==============================

def _safe_text(v):
    """Renvoie une chaîne sûre (jamais NaN, None, etc.) pour le XML."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v)

def _safe_filename(s, maxlen=60):
    """Nettoie le nom du fichier pour le ZIP."""
    s2 = _safe_text(s)
    s2 = re.sub(r'[\\/:"*?<>|]+', '_', s2)
    s2 = s2.strip()
    if len(s2) > maxlen:
        s2 = s2[:maxlen].rstrip()
    if not s2:
        s2 = "untitled"
    return s2


# ==============================
# Extraction des auteurs OpenAlex
# ==============================

def extract_authors_from_openalex_json(openalex_authorships):
    """
    Extrait la liste des auteurs depuis la structure OpenAlex JSON (champ 'authorships').
    Chaque auteur contient name, orcid et raw affiliations.
    """
    authors_data = []

    if not openalex_authorships:
        return authors_data

    for auth in openalex_authorships:
        try:
            author_entry = {
                "name": _safe_text(auth.get("raw_author_name", "")),
                "orcid": _safe_text(auth.get("author", {}).get("orcid", "")),
                "raw_affiliations": []
            }

            for aff in auth.get("institutions", []):
                raw_aff_text = aff.get("display_name", "")
                if raw_aff_text:
                    author_entry["raw_affiliations"].append(raw_aff_text)

            authors_data.append(author_entry)
        except Exception as e:
            st.warning(f"Erreur dans extract_authors_from_openalex_json : {e}")
            continue

    return authors_data


# ==============================
# Génération XML HAL (un par publication)
# ==============================

def generate_hal_xml(pub_data):
    """Génère un fichier XML HAL (format TEI) pour une publication donnée."""
    TEI = ET.Element(
        "TEI",
        {
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xmlns": "http://www.tei-c.org/ns/1.0",
            "xmlns:hal": "http://hal.archives-ouvertes.fr/",
            "xsi:schemaLocation": "http://www.tei-c.org/ns/1.0 http://api.archives-ouvertes.fr/documents/aofr-sword.xsd",
        },
    )

    text = ET.SubElement(TEI, "text")
    body = ET.SubElement(text, "body")
    listBibl = ET.SubElement(body, "listBibl")
    biblFull = ET.SubElement(listBibl, "biblFull")

    titleStmt = ET.SubElement(biblFull, "titleStmt")
    seriesStmt = ET.SubElement(biblFull, "seriesStmt")
    notesStmt = ET.SubElement(biblFull, "notesStmt")
    ET.SubElement(notesStmt, "note", {"type": "peer", "n": "1"}).text = "Yes"

    sourceDesc = ET.SubElement(biblFull, "sourceDesc")
    biblStruct = ET.SubElement(sourceDesc, "biblStruct")
    analytic = ET.SubElement(biblStruct, "analytic")

    # --- Titre principal ---
    ET.SubElement(analytic, "title", {"xml:lang": "en"}).text = _safe_text(pub_data.get("Title", ""))

    # --- Auteurs ---
    for author in pub_data.get("authors", []):
        author_el = ET.SubElement(analytic, "author", {"role": "aut"})
        persName = ET.SubElement(author_el, "persName")

        name_parts = _safe_text(author.get("name")).split(" ", 1)
        if len(name_parts) == 2:
            ET.SubElement(persName, "forename", {"type": "first"}).text = name_parts[0]
            ET.SubElement(persName, "surname").text = name_parts[1]
        else:
            ET.SubElement(persName, "surname").text = _safe_text(author.get("name"))

        if author.get("orcid"):
            ET.SubElement(author_el, "idno", {"type": "ORCID"}).text = _safe_text(author["orcid"])

        for raw_aff in author.get("raw_affiliations", []):
            ET.SubElement(author_el, "rawAffs").text = _safe_text(raw_aff)

    # --- Monographie / journal ---
    monogr = ET.SubElement(biblStruct, "monogr")
    ET.SubElement(monogr, "title", {"level": "j"}).text = _safe_text(pub_data.get("Source title", ""))
    imprint = ET.SubElement(monogr, "imprint")
    ET.SubElement(imprint, "publisher").text = _safe_text(pub_data.get("publisher", ""))
    ET.SubElement(imprint, "date", {"type": "datePub"}).text = _safe_text(pub_data.get("Date", ""))

    # --- DOI ---
    if pub_data.get("doi"):
        ET.SubElement(biblStruct, "idno", {"type": "doi"}).text = _safe_text(pub_data.get("doi", ""))

    # --- Retour XML bytes ---
    xml_bytes = ET.tostring(TEI, encoding="utf-8", xml_declaration=True)
    return xml_bytes


# ==============================
# Génération du ZIP global
# ==============================

def generate_zip_from_xmls(publications_list):
    """
    Génère un fichier ZIP en mémoire contenant un fichier XML par publication.
    Retourne un objet BytesIO prêt à être téléchargé.
    """
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for pub in publications_list:
            try:
                xml_bytes = generate_hal_xml(pub)
                xml_filename = f"{_safe_filename(pub.get('Title', 'untitled'))}.xml"
                zip_file.writestr(xml_filename, xml_bytes)
            except Exception as e:
                st.warning(f"Erreur lors de la génération XML pour '{_safe_text(pub.get('Title', ''))}': {e}")
                continue

    zip_buffer.seek(0)
    return zip_buffer
