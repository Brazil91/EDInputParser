# -*- coding: utf-8 -*-
from .__init__ import pp, os, re

digits = 4
ojnPattern = re.compile(r"A8(\d+)")
filenamePattern = re.compile(r"([A-Za-z0-9_.-]+\.(edi|xml))")
grossWeightPattern = re.compile(r"MEA\+AAE\+G\+KGM:(\d+\.?\d*)")
woPattern = re.compile(r"BGM\+\d+\+([A-Za-z0-9_-]+)\+\d+")
unaPattern = re.compile(r"UNA:.*?'")


# Function to extract weights and order number from an EDI file
def maeCommodityWeightAnalysis(content):
    commodity_weights = []
    commodityEndBool = 0
    filename = "Unknown"
    order_number = "Unknown"    
    net_weight = "Unknown"
    ojn = "Unknown"
    for line in content.split("'"):    
        
        ojn_match = ojnPattern.search(line)
        if ojn_match:
            ojn = ojn_match.group(1)

        filename_match = filenamePattern.search(line)
        if filename_match:
            filename = filename_match.group(1)

        # Extract order number (BGM+340)
        order_number_match = re.search(r"RFF\+TON\:(\d+)'?",line)
        if order_number_match:
            order_number = order_number_match.group(1)
        
        if "EQD+CN" in line:
            commodityEndBool = 1
        
        # Extract commodity weights (MEA+AAE+G)
        if commodityEndBool == 0:
            commodity_weight_match = grossWeightPattern.search(line)
            if commodity_weight_match:
                commodity_weights.append(float(commodity_weight_match.group(1)))
                commodity_weight_match = None
            
        # Extract container net weight (MEA+AAE+AAL)
        net_weight_match = re.search(r"MEA\+AAE\+AAL\+KGM:(\d+\.?\d*)", line)
        if net_weight_match:
            net_weight = round(float(net_weight_match.group(1)) / 1000, digits) if net_weight_match else 0

    accCommodityWeights = round(sum(commodity_weights) / 1000, digits) if len(commodity_weights) > 0 else 0
    # pp(f'containerNet == commoditySum: {accCommodityWeights == net_weight}')
    
    return {
        "Filename": filename,
        "WO": order_number,
        "ContainerNetWeight in t": net_weight,
        "CommodityWeightSum in t": accCommodityWeights,
        "CommodityCount": len(commodity_weights),
        "AccCommodityWeightSum-NetWeights in t": round(accCommodityWeights - net_weight, digits),
        "WeightsConsistent": accCommodityWeights == net_weight,
        "OJN": ojn,
    }

def saveXML(sftp, content, remote_path, local_path, ton, filename, ojn, customer):
        
    data = []
    xmlStarter = 0
    discard = 0
    customerSpecifics = {
        "stakeholder2": "<jobItemMessage",
        "stakeholder1Dak": "<Document",
    }
    startMatchNotComplete = 0

    endTag = "".join([x if i > 0 else x + "/" for i, x in enumerate(customerSpecifics[customer])])

    for line in content.replace("\r","").split("\n"):
        startMatch = re.search(customerSpecifics[customer], line.strip())
        endMatch = re.search(endTag, line.strip())
        if line.strip() == "":
            continue

        if xmlStarter:
            if endMatch:
                data.append("".join(line.split(">")[:-1]) + ">")
                break
            else:
                if customer == "stakeholder2":
                    if  "<jobItemMessageType>Update" in line.strip():
                        discard = 1
                        break
                if startMatchNotComplete:
                    if re.search(">", line.strip()):
                        data.append(startMatchNotComplete + " " + "".join(line.strip().split(">")[:-1]) + ">")
                        startMatchNotComplete = 0
                else:
                    data.append(line.rstrip())
                continue

        else:
            if startMatch:
                if startMatch.string.endswith(">"):
                    data.append(startMatch.group(1))
                else: 
                    startMatchNotComplete = startMatch.string
                xmlStarter = 1
                continue
        
    if discard:
        print(f"Discarding {filename} due to Update jobItemMessageType")
        return
    
    if ton is None or ton == "":
        orderNumberSubstring = ""
    else:
        orderNumberSubstring = f"_{ton}"
    
    savingPath = local_path / ("".join(filename.split(".")[:-1]) + f"{orderNumberSubstring}_{ojn}.xml")
    
    # if filename.endswith(".dat"): 
    #     savingPath = local_path / filename.replace(".dat", f"{orderNumberSubstring}_{ojn}.xml")
    # elif filename.endswith(".xml"):
    #     savingPath = local_path / filename.replace(".xml", f"{orderNumberSubstring}_{ojn}.xml")
    # elif filename.endswith(".XML"):
    #     savingPath = local_path / filename.replace(".xml", f"{orderNumberSubstring}_{ojn}.xml")

    ediCleaned = "\n".join(data)

    with open(savingPath, "w", encoding="utf-8") as f:
        f.write(ediCleaned)

def saveIFTMIN(sftp, content, remote_path, local_path, ton, filename, **kwargs):
    
    
    ediStarter = 0
    data = []
    
    for line in content.replace("\r","").split("\n"):    
        if line == "":
            continue
        
        if not unaPattern.search(line) and ediStarter == 0:
            continue
        
        if unaPattern.search(line):
            ediStarter = 1
            data.append(unaPattern.search(line).group())
         
        elif line.startswith("UNZ"):
            data.append(line)

        else:
            data.append(line)

    ponSubset = f"_pon_{kwargs['references'].get('pon')}" if kwargs['references'].get('pon', 0) else ""
    nonSubset = f"_non_{kwargs['references'].get('non')}" if kwargs['references'].get('non', 0) else ""
    tonSubset = f"_wo_{kwargs['references'].get('ton')}" if kwargs['references'].get('ton', 0) else ""
    ojnSubset = f"_{kwargs['references'].get('ojn')}" if kwargs['references'].get('ojn', 0) else ""

    savingPath = local_path / filename.replace(".edi", f"{tonSubset}{ponSubset}{nonSubset}{ojnSubset}.edi")
    ediCleaned = "\n".join(data)
    
    with open(savingPath, "w", encoding="utf-8") as f:
        f.write(ediCleaned)
    
def maeFetchCustomsProcedureOrders(sftp, content, remote_path, local_path, ton, filename, ojn):
    
    
    ediStarter = 0
    data = []
    
    for line in content.replace("\r","").split("\n"):    
        if line == "":
            continue
        
        if not unaPattern.search(line) and ediStarter == 0:
            continue
        
        if unaPattern.search(line):
            ediStarter = 1
            data.append(unaPattern.search(line).group())
         
        elif line.startswith("UNZ"):
            data.append(line)

        else:
            data.append(line)

    savingPath = local_path / filename.replace(".edi", f"_{ton}_{ojn}.edi")
    ediCleaned = "\n".join(data)
    
    with open(savingPath, "w", encoding="utf-8") as f:
        f.write(ediCleaned)
    
    #! Download
    #? sftp.get(remote_path, local_path)
        
results = []

def mailGen(content):
    total_commodity_weight, commoditysCount, container_net_weight, order_number, filename = maeCommodityWeightAnalysis(content.replace("\n", "").replace("\r", ""))
    results.append((filename, total_commodity_weight, container_net_weight, order_number))

    # Compose the email
    email_body = "Subject: EDI Weight Comparison Results\n\n"
    email_body += "Dear Team,\n\nHere are the results of the weight comparison for the EDI files:\n\n"

    for filename, total_commodity_weight, container_net_weight, order_number in results:
        comparison = "MATCH" if total_commodity_weight == container_net_weight else "MISMATCH"
        email_body += f"- File: {filename}\n"
        email_body += f"  Work Order: {order_number}\n"
        email_body += f"  Total Commodity Weight: {total_commodity_weight} KGM\n"
        email_body += f"  Container Net Weight: {container_net_weight} KGM\n"
        email_body += f"  Result: {comparison}\n\n"

    email_body += "Best regards,\nYour IT-System"
    
    return email_body

# Print the email body
# print(email_body)