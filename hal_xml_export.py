import xml.etree.ElementTree as ET
import io
import zipfile
import pandas as pd
import streamlit as st

# --- Fonction 1 : extraction des auteurs et affiliations depuis OpenAlex ---
def extract_authors_from_openalex_json(openalex_json):
    """
    Extrait les auteurs et leurs affiliations à partir des données OpenAlex (champ 'authorships').
    Chaque auteur est un dict : {name, orcid, raw_affiliations, ror_affiliations}.
    """
    authors_list = []

    try:
        # OpenAlex renvoie parfois une liste complète de résultats
        if isinstance(openalex_json, dict) and "results" in openalex_json:
            works = openalex_json["results"]
        elif isinstance(openalex_json, list):
            works = openalex_json
        else:
            works = [openalex_json]

        for work in works:
            for authorship in work.get("authorships", []):
                author_entry = {}

                # --- Nom brut (raw_author_name plutôt que display_name)
                author_entry["name"] = authorship.get("raw_author_name") or \
                                       (authorship.get("author", {}).get("display_name", ""))

                # --- ORCID (si disponible)
                author_entry["orcid"] = authorship.get("author", {}).get("orcid", "")

                # --- Affiliations brutes (liste de chaînes)
                raw_affiliations = authorship.get("raw_affiliation_strings", [])
                author_entry["raw_affiliations"] = [str(a) for a in raw_affiliations if a]

                # --- Institutions avec ROR
                institutions = authorship.get("institutions", [])
                ror_list = []
                for inst in institutions:
                    ror_list.append({
                        "ror": inst.get("ror", ""),
                        "display_name": inst.get("display_name", "")
                    })
                author_entry["ror_affiliations"] = ror_list

                authors_list.append(author_entry)
    except Exception as e:
        st.warning(f"Erreur lors de l'extraction des auteurs OpenAlex : {e}")

    return authors_list


# --- Fonction 2 : génération du fichier XML HAL pour une publication ---
def generate_hal_xml(pub_data):
    """
    Génère un fichier XML HAL (au format TEI) pour une publication donnée.
    pub_data doit contenir au minimum :
      - Title
      - doi
      - authors (liste issue d'extract_authors_from_openalex_json)
    """
    TEI = ET.Element("TEI", {
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xmlns": "http://www.tei-c.org/ns/1.0",
        "xmlns:hal": "http://hal.archives-ouvertes.fr/",
        "xsi:schemaLocation": "http://www.tei-c.org/ns/1.0 http://api.archives-ouvertes.fr/documents/aofr-sword.xsd"
    })

    text = ET.SubElement(TEI, "text")
    body = ET.SubElement(text, "body")
    listBibl = ET.SubElement(body, "listBibl")
    biblFull = ET.SubElement(listBibl, "biblFull")

    # --- Metadata structure
    titleStmt = ET.SubElement(biblFull, "titleStmt")
    seriesStmt = ET.SubElement(biblFull, "seriesStmt")
    notesStmt = ET.SubElement(biblFull, "notesStmt")
    ET.SubElement(notesStmt, "note", {"type": "peer", "n": "1"}).text = "Yes"

    sourceDesc = ET.SubElement(biblFull, "sourceDesc")
    biblStruct = ET.SubElement(sourceDesc, "biblStruct")

    analytic = ET.SubElement(biblStruct, "analytic")

    # --- Titre de l’article
    ET.SubElement(analytic, "title", {"xml:lang": "en"}).text = str(pub_data.get("Title", "") or "")

    # --- Auteurs
    authors = pub_data.get("authors", [])
    for author in authors:
        author_el = ET.SubElement(analytic, "author", {"role": "aut"})
        persName = ET.SubElement(author_el, "persName")

        # Découpage prénom/nom simple
        if author.get("name"):
            name_parts = author["name"].split(" ", 1)
            if len(name_parts) == 2:
                ET.SubElement(persName, "forename", {"type": "first"}).text = name_parts[0]
                ET.SubElement(persName, "surname").text = name_parts[1]
            else:
                ET.SubElement(persName, "surname").text = author["name"]

        # ORCID
        if author.get("orcid"):
            ET.SubElement(author_el, "idno", {"type": "ORCID"}).text = author["orcid"]

        # rawAffiliations
        for raw_aff in author.get("raw_affiliations", []):
            ET.SubElement(author_el, "rawAffs").text = raw_aff

        # Affiliations ROR
        for i, aff in enumerate(author.get("ror_affiliations", [])):
            if aff.get("ror"):
                ET.SubElement(author_el, "affiliation", {"ref": f"#localStruct-Aff{i+1}"})

    # --- Source de la publication
    monogr = ET.SubElement(biblStruct, "monogr")
    ET.SubElement(monogr, "title", {"level": "j"}).text = pub_data.get("Source title", "")
    imprint = ET.SubElement(monogr, "imprint")
    ET.SubElement(imprint, "publisher").text = pub_data.get("publisher", "")
    ET.SubElement(imprint, "date", {"type": "datePub"}).text = str(pub_data.get("Date", ""))

    ET.SubElement(biblStruct, "idno", {"type": "doi"}).text = pub_data.get("doi", "")

    # --- Ajout des structures organisations (listOrg)
    back = ET.SubElement(text, "back")
    listOrg = ET.SubElement(back, "listOrg", {"type": "structures"})

    all_rors = []
    for author in authors:
        for i, aff in enumerate(author.get("ror_affiliations", [])):
            if aff.get("ror") and aff.get("display_name") and aff["ror"] not in all_rors:
                org = ET.SubElement(listOrg, "org", {"type": "institution", "xml:id": f"localStruct-Aff{i+1}"})
                ET.SubElement(org, "idno", {"type": "ROR"}).text = aff["ror"]
                ET.SubElement(org, "orgName").text = aff["display_name"]
                all_rors.append(aff["ror"])

    xml_bytes = ET.tostring(TEI, encoding="utf-8", xml_declaration=True)
    return xml_bytes


# --- Fonction 3 : génération d’un ZIP contenant tous les fichiers XML ---
def generate_zip_from_xmls(publications_list):
    """
    Prend une liste de publications (avec leurs métadonnées) et renvoie un fichier ZIP en mémoire.
    """
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for pub in publications_list:
            try:
                xml_bytes = generate_hal_xml(pub)
                safe_title = (pub.get("Title", "untitled") or "untitled").replace("/", "_")[:60]
                xml_filename = f"{safe_title}.xml"
                zip_file.writestr(xml_filename, xml_bytes)
            except Exception as e:
                st.warning(f"Erreur lors de la génération XML pour '{pub.get('Title', '')}': {e}")
    zip_buffer.seek(0)
    return zip_buffer
