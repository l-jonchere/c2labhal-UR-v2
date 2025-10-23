import xml.etree.ElementTree as ET
import io
import zipfile

def generate_hal_xml(publication):
    """
    Génère un fichier XML HAL-TEI pour une publication donnée.
    publication : dict avec au moins :
      Title, doi, Date, Source title, publisher, authors (liste), raw_affiliations (liste)
    """

    NS = {
        None: "http://www.tei-c.org/ns/1.0",
        "hal": "http://hal.archives-ouvertes.fr/"
    }

    # --- Racine TEI ---
    TEI = ET.Element("TEI", {
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xsi:schemaLocation": "http://www.tei-c.org/ns/1.0 http://api.archives-ouvertes.fr/documents/aofr-sword.xsd"
    })

    # --- Arborescence principale ---
    text = ET.SubElement(TEI, "text")
    body = ET.SubElement(text, "body")
    listBibl = ET.SubElement(body, "listBibl")
    biblFull = ET.SubElement(listBibl, "biblFull")

    # === titleStmt / seriesStmt / notesStmt ===
    ET.SubElement(biblFull, "titleStmt")
    ET.SubElement(biblFull, "seriesStmt")

    notesStmt = ET.SubElement(biblFull, "notesStmt")
    ET.SubElement(notesStmt, "note", {"type": "audience", "n": "2"})
    ET.SubElement(notesStmt, "note", {"type": "popular", "n": "0"}).text = "No"
    ET.SubElement(notesStmt, "note", {"type": "peer", "n": "1"}).text = "Yes"

    # === sourceDesc / biblStruct ===
    sourceDesc = ET.SubElement(biblFull, "sourceDesc")
    biblStruct = ET.SubElement(sourceDesc, "biblStruct")
    analytic = ET.SubElement(biblStruct, "analytic")

    # --- Titre ---
    title_el = ET.SubElement(analytic, "title", {"xml:lang": "en"})
    title_el.text = publication.get("Title", "")

    # --- Auteurs ---
    # On mappe d'abord toutes les affiliations uniques pour leur donner un xml:id stable
    raw_affs = sorted(set(publication.get("raw_affiliations", [])))
    aff_id_map = {aff: f"localStruct-Aff{i+1}" for i, aff in enumerate(raw_affs)}

    for author in publication.get("authors", []):
        aut_el = ET.SubElement(analytic, "author", {"role": "aut"})
        pers = ET.SubElement(aut_el, "persName")
        ET.SubElement(pers, "forename", {"type": "first"}).text = author.get("forename", "")
        ET.SubElement(pers, "surname").text = author.get("surname", "")
        if author.get("orcid"):
            ET.SubElement(aut_el, "idno", {"type": "ORCID"}).text = author["orcid"]

        # Affiliation brute (rawAffs)
        for aff_text in author.get("affiliations", []):
            ET.SubElement(aut_el, "rawAffs").text = aff_text
            # Si correspondance avec structure connue, on lie avec un ref
            if aff_text in aff_id_map:
                ET.SubElement(aut_el, "affiliation", {"ref": f"#{aff_id_map[aff_text]}"})

    # --- Journal ---
    monogr = ET.SubElement(biblStruct, "monogr")
    ET.SubElement(monogr, "title", {"level": "j"}).text = publication.get("Source title", "")
    imprint = ET.SubElement(monogr, "imprint")
    ET.SubElement(imprint, "publisher").text = publication.get("publisher", "")
    ET.SubElement(imprint, "date", {"type": "datePub"}).text = str(publication.get("Date", ""))

    # --- Identifiants (DOI, PMID, etc.) ---
    if publication.get("doi"):
        ET.SubElement(biblStruct, "idno", {"type": "doi"}).text = publication["doi"]
    if publication.get("pubmed"):
        ET.SubElement(biblStruct, "idno", {"type": "pubmed"}).text = str(publication["pubmed"])

    # === profileDesc (langue, typologie HAL, mots-clés) ===
    profileDesc = ET.SubElement(biblFull, "profileDesc")

    langUsage = ET.SubElement(profileDesc, "langUsage")
    ET.SubElement(langUsage, "language", {"ident": "en"}).text = "English"

    textClass = ET.SubElement(profileDesc, "textClass")
    ET.SubElement(textClass, "classCode", {"scheme": "halTypology", "n": "ART"}).text = "Journal articles"

    # Mots-clés optionnels
    keywords = publication.get("keywords", [])
    if keywords:
        kw_el = ET.SubElement(textClass, "keywords", {"scheme": "author"})
        for kw in keywords:
            ET.SubElement(kw_el, "term", {"xml:lang": "en"}).text = kw

    # --- Résumé optionnel ---
    abstract_el = ET.SubElement(profileDesc, "abstract", {"xml:lang": "en"})
    abstract_el.text = publication.get("abstract", "")

    # === Structures d'affiliation ===
    back = ET.SubElement(text, "back")
    listOrg = ET.SubElement(back, "listOrg", {"type": "structures"})

    for aff_text, aff_id in aff_id_map.items():
        org_el = ET.SubElement(listOrg, "org", {"type": "institution", "xml:id": aff_id})
        ET.SubElement(org_el, "orgName").text = aff_text
        desc_el = ET.SubElement(org_el, "desc")
        addr_el = ET.SubElement(desc_el, "address")
        ET.SubElement(addr_el, "country", {"key": "FR"})

    # --- Sérialisation propre ---
    xml_bytes = ET.tostring(TEI, encoding="utf-8", xml_declaration=True)
    return xml_bytes
    
def generate_zip_from_xmls(publications_list):
    """
    Crée un fichier ZIP contenant un XML HAL-TEI par publication.
    publications_list : liste de tuples (id_pub, dict_publication)
    """
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for pub_id, pub_data in publications_list:
            xml_bytes = generate_hal_xml(pub_data)
            zf.writestr(f"{pub_id}.xml", xml_bytes)
    zip_buffer.seek(0)
    return zip_buffer
