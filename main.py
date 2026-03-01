# -*- coding: utf-8 -*-
import src.ediProcessing as ep
import src.paramikoFunctions as pf
from src import colored, tqdm, ExcelWriter, copy, dt, root, pp, yaml, pd, os, paramiko, os, re, pl, sys, createRandomSample, br, cfg, pp


from dotenv import load_dotenv
import numpy as np
load_dotenv(root / "configs" /".env")

hostProd = os.getenv("hostProd")
userProd = os.getenv("userProd")
pwProd = os.getenv("pwProd")
archiveRoot = os.getenv("archiveRoot")
    
local_dir = root / "results"  # Lokales Verzeichnis

class SFTP_FileProcessor:
    def __init__(self, sftp, remoteEingang, local_path, customer, *args, **kwargs):
        self.sftp = sftp
        self.remoteEingang = remoteEingang
        self.local_path = local_path
        self.customer = customer
        self.counter = 0
        
        
        if kwargs.get("inputReferenceList", 0): 
            self.dataDict = {f"{x}": 1 for x in kwargs.get("inputReferenceList")}

    def processFiles(self, processorType, **kwargs):
        """Process all files in the remote directory."""
        
        if processorType not in ["stakeholder1PONandNON"]:
            
            if not self.dataDict:
                print(f"\nNo inputReferenceList provided, exiting!")
                sys.exit(1)

            osisArchiveFiles = sorted(self.sftp.listdir(self.remoteEingang), reverse=True)
            
            for n, filename in enumerate(tqdm(
                osisArchiveFiles,
                total=len(osisArchiveFiles)
            )):
                
                if self.dataDict:
                    if len(self.dataDict) == 0:
                        print(colored(f"All {self.counter} files processed, no more data to process.", "magenta"))
                        break
                    
                if not self.processFileRemote(filename, processorType, **kwargs):
                    continue  # Skip the outer loop iteration

            print(colored(f"Processed {self.counter} files, {len(self.dataDict)} missing of {len(kwargs.get('inputReferenceList', []))}.", "magenta"))

        elif content and processorType == "stakeholder1PONandNON":
            print("\nEntering stakeholder1PONandNON processing with content provided...")
            self.processFileContent(kwargs["filename"], processorType, content=content, **kwargs)

    def processorTypeREHandler(self, processorType, matchCondition, key):
        """
        Generic function to handle data processing based on processor type.

        Args:
            processorType (str): Type of processor (e.g., "specificWOs", "specificOJNs").
            matchCondition (bool): Condition to check for processing.
            key (str): Key to check and delete from the data dictionary.

        Returns:
            bool: True if processing is successful, False otherwise.
        """
        if matchCondition and self.dataDict.get(key, 0):
            del self.dataDict[key]
            self.counter += 1
            return False
        else:
            return True
    
    def processFileContent(self, filename, processorType, *args, **kwargs):
        
        content = kwargs["content"]
        
        references = kwargs.get("References", {})
        ton = references.get("ton", None)
        pon = references.get("pon", None)
        ojn = references.get("ojn", None)
        non = references.get("non", None)
        
        
        ep.saveIFTMIN(self.sftp, content, remote_path, self.local_path, ton, filename, ojn)

        
    def processFileRemote(self, filename, processorType, *args, **kwargs):
        """Process a single file and return whether to continue the outer loop."""
        if filename.endswith(".arc"):
            return False  # Skip outer loop for this file
        
        remote_path = self.remoteEingang + "/" + filename
        references = {}
        ton = None
        try:
            with self.sftp.open(remote_path, 'r') as remote_file:
                content = remote_file.read().decode(errors="ignore")
                for line in content.replace("\n", "").replace("\r", "").split("'"):
                    
                    ojnMatch = ep.ojnPattern.search(line)
                    if ojnMatch:
                        ojn = ojnMatch.group(1)

                        if processorType == "specificOJNs":
                            breakLoop = self.processorTypeREHandler(processorType, ojnMatch, f"{ojn}")
                            if breakLoop:
                                return False
                            else:
                                references |= {"ojn": ojn}
                                break
                        
                    orderNumberMatch = ep.woPattern.search(line)
                    if orderNumberMatch:
                        ton = orderNumberMatch.group(1)                        
                        
                        if processorType == "specificWOs":
                            breakLoop = self.processorTypeREHandler(processorType, orderNumberMatch, f"{ton}")
                            if breakLoop:
                                return False
                            else:
                                references |= {"ton": ton}
                                break
                                
                    if ep.filenamePattern.search(line):
                        filename = ep.filenamePattern.search(line).group(1)
                
                if self.customer == "stakeholder1":
                    ep.saveIFTMIN(self.sftp, content, remote_path, self.local_path, ton, filename, references=references)
                elif self.customer in ["stakeholder2", "stakeholder1Dak"]:
                    ep.saveXML(self.sftp, content, remote_path, self.local_path, ton, filename, ojn, self.customer)
                
                print(colored(f"\nProcessed {self.counter} files, {len(self.dataDict)} missing of {len(self.dataDict)}.", "magenta"))
                notFoundOrdersList = list(self.dataDict.keys())
                
                if len(notFoundOrdersList) != 0:
                    print(f"\nOrders not received:\n\n{notFoundOrdersList}")

        except OSError as e:
            print(f"Error reading file {remote_path}: {e}")
            return False
        return True

if __name__ == "__main__":

    ioSelector = input("Select I/O procedure (1 for specificOrders, 2 for randomSample, 3 for customsProcedureIssueBool, 4 for commodityIssueBool, 5 for stakeholder1PONandNON): " or "specificOrders").strip()

    if ioSelector == "1":
        processorType = input("\nSelect inputReferenceList type (1 for specificOJNs, 2 for specificWOs): " or 1).strip()
        if processorType == "1":
            print("\nUsing OJN's as references...")
        elif processorType == "2":
            print("\nUsing WO's as references...")
        else:
            print("\nInvalid selection, exiting...")
            sys.exit(1)
            
        # Input reference list, split into multiple lines for readability
        inputReferenceList = [
            'XXX',
        ]

        processorType = "specificOJNs" if processorType == "1" else ("specificWOs" if processorType == "2" else None)

        if not input("Input references up to date (1/0) ? " or 1):
            print("\nExiting...")
            sys.exit(1)
            
    elif ioSelector == "2":
        sampleSize = int(input("\nEnter sample size (default 20): ") or 20)
        print("\nUsing random sample with sample size of 20")
    
    if ioSelector in ["3", "4", "5"]:
        print("\nCustomer stakeholder1 is selected by default for this procedure...")
        customerSelector = "2"
    else:
        customerSelector = input("\nEnter customer (1 for stakeholder2, 2 for stakeholder1, 3 for stakeholder1Dak): ").strip()
            
    sftpProcedures = cfg['sftpProcedures']
    customerPath = cfg['customerPath']
    
    remoteEingang = f"{archiveRoot}{customerPath[cfg['customer'][customerSelector]]}"
    
    print("\nStarting EDI processing...")

    with pf.transportConnector(hostProd, userProd, pwProd) as transport:
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            
            if "specificOrders" == cfg["sftpProcedures"][ioSelector]:
                print("\nStarting to process specific orders...")
                
                local_path = local_dir / "specificOrders"
                br.mkDir(local_path)
                remoteEingang = f"{archiveRoot}{customerPath[cfg['customer'][customerSelector]]}"
                                
                if "stakeholder2" == cfg['customer'][customerSelector]:
                    print("\nProcessing stakeholder2 specific orders...")
                
                if "stakeholder1" == cfg['customer'][customerSelector]:
                    print("\nProcessing stakeholder1 specific orders...")

                processor = SFTP_FileProcessor(sftp, remoteEingang, local_path, cfg['customer'][customerSelector], inputReferenceList=inputReferenceList)
                processor.processFiles(processorType)

            if "stakeholder1PONandNON" == cfg["sftpProcedures"][ioSelector]:
                
                processorType = "stakeholder1PONandNON"
                
                local_path = local_dir / processorType
                br.mkDir(local_path)
                print("\nStarting to process stakeholder1 files regarding PON and/or NON references...")
                
                inputArchiveFiles = sftp.listdir(remoteEingang)
                
                inputArchiveFilesLen = len(inputArchiveFiles)
                
                stats = {}
                tonDict = {}
                ponDict = {}
                nonDict = {}
                ponNonList = {}
                
                for filename in tqdm(
                    inputArchiveFiles
                    ,total=inputArchiveFilesLen
                ):
                    if filename.endswith(".arc"):
                        continue
                    
                    ojn = ""
                    ton = ""     
                    pon = None
                    non = None
                    references = {}
                    
                    remote_path = remoteEingang + "/" + filename
                    
                    with sftp.open(remote_path, 'r') as remote_file:
                                
                        content = remote_file.read().decode(errors="ignore")
                        for line in content.replace("\n", "").replace("\r", "").split("'"):
                            
                            ojnMatch = ep.ojnPattern.search(line)
                            if ojnMatch:
                                references |= {"ojn": ojnMatch.group(1)}    
                            
                            if "RFF+TON" in line.strip():
                                ton = line.split(':')[1].strip()
                                references |= {"ton": ton}
                                tonDict |= {f"{filename}_{ton}": ton}
                                
                            if ep.filenamePattern.search(line):
                                filename = ep.filenamePattern.search(line).group(1)
                            
                            if "RFF+PON" in line.strip():
                                pon = line.split(':')[1].strip()
                                references |= {"pon": pon}
                                ponDict |= {f"{filename}_{pon}": ton}

                            if "RFF+NON" in line.strip():
                                non = line.split(':')[1].strip()
                                references |= {"non": non}
                                nonDict |= {f"{filename}_{non}": ton}

                            if line.strip().startswith("TDT+20"):
                                if references.get("pon", 0) or references.get("non", 0):
                                    
                                    # if references.get("pon", 0) and references.get("non", 0):
                                    #     ponNonList.append({filename: {"pon": references.get("pon"), "non": references.get("non")}})
                                    #     del ponDict[f"{filename}_{pon}"]
                                    #     del nonDict[f"{filename}_{non}"]

                                    ep.saveIFTMIN(sftp, content, remote_path, local_path, ton, filename, references=references)
                
                stats |= {"Stats description":[
                        "EDIs in input archive analyzed"
                        ,"Previous FRO count"
                        ,"Next FRO count"
                        ,"Previous PON and NON"
                        ,"No PON, NON or both count"
                        ,""
                    ]
                    ,"Stats values": [
                        inputArchiveFilesLen
                        ,len(ponDict)
                        ,len(nonDict)
                        ,len(ponNonList)
                        ,inputArchiveFilesLen - (len(ponDict) + len(nonDict) + len(ponNonList))
                        ,""
                    ]   
                }
                
                dfStats = pd.DataFrame(data=stats)
                dfPON = pd.DataFrame.from_dict(ponDict, orient='index', columns=['WO die PON enthalten'])
                dfNON = pd.DataFrame.from_dict(nonDict, orient='index', columns=['WO die NON enthalten'])
                dfPONandNON = pd.DataFrame(ponNonList, columns=['WO die PON und NON enthalten'])

                # Join the DataFrames, aligning by index, filling missing values with np.nan
                dfJoined = pd.concat([dfStats, dfPON, dfNON, dfPONandNON], axis=1)
                dfJoined = dfJoined.apply(lambda col: pd.Series(col.dropna().values), axis=0)

                # # Fill missing values with np.nan (should already be the default, but explicit)
                # dfJoined = dfJoined.where(pd.notnull(dfJoined), np.nan)
                print()
                pp(dfJoined)
                br.ExcelWriter(
                    local_dir / f"stakeholder1_PON_NON_stats_{dt.datetime.now().strftime('%F_%T')}.xlsx".replace(":", "-")
                    ,dfDict={"Results": dfJoined}
                )


            if "randomSample" == cfg["sftpProcedures"][ioSelector]:
                
                if cfg['customer'][customerSelector] == "stakeholder1":
                    local_path = local_dir / "iftminRandomSamples" /"stakeholder1"
                else:
                    local_path = local_dir / "RandomSamples"
                br.mkDir(local_path)
                
                print("\nStarting to process random sample of IFTMIN files...")
                
                randomSample = createRandomSample(sftp.listdir(remoteEingang), sampleSize=sampleSize)
                
                
                for filename in tqdm(
                    randomSample
                    ,total=len(randomSample)
                ):
                    if filename.endswith(".arc"):
                        continue
                    
                    ojn = "Unknown"
                    ton = "Unknown"     
                    references = {}                
                    stats = {}
                    tonDict = {}
                    ponDict = {}
                    nonDict = {}
                    ponNonList = {}
                    
                    remote_path = remoteEingang + "/" + filename
                    
                    with sftp.open(remote_path, 'r') as remote_file:
                                
                        content = remote_file.read().decode(errors="ignore")
                        for line in content.replace("\n", "").replace("\r", "").split("'"):
                            
                            ojnMatch = ep.ojnPattern.search(line)
                            if ojnMatch:
                                references |= {"ojn": ojnMatch.group(1)}    
                            
                            if "RFF+TON" in line.strip():
                                ton = line.split(':')[1].strip()
                                references |= {"ton": ton}
                                tonDict |= {f"{filename}_{ton}": ton}
                                
                            if ep.filenamePattern.search(line):
                                filename = ep.filenamePattern.search(line).group(1)
                            
                            if "RFF+PON" in line.strip():
                                pon = line.split(':')[1].strip()
                                references |= {"pon": pon}
                                ponDict |= {f"{filename}_{pon}": ton}

                            if "RFF+NON" in line.strip():
                                non = line.split(':')[1].strip()
                                references |= {"non": non}
                                nonDict |= {f"{filename}_{non}": ton}

                            if line.strip().startswith("TDT+20"):
                                ep.saveIFTMIN(sftp, content, remote_path, local_path, ton, filename, references=references)
                                continue
                            # ojnMatch = ep.ojnPattern.search(line)
                            # if ojnMatch:
                            #     ojn = ojnMatch.group(1)
                                
                            
                            # if ep.woPattern.search(line):
                            #     ton = ep.woPattern.search(line).group(1)
                                
                            # if ep.filenamePattern.search(line):
                            #     filename = ep.filenamePattern.search(line).group(1)
                                
                            # if "BGM+" in line:
                            #     break

                        # ep.saveIFTMIN(sftp, content, remote_path, local_path, ton, filename, ojn)

            if "customsProcedureIssueBool" == cfg["sftpProcedures"][ioSelector]:
                print("\nStarting to get customs procedure issue orders...")
                remoteAusgang = f"{archiveRoot}/ausgang/{cfg['ausgang']['MAE']['MAE_IFTMIN_no_procedure']}"
                
                print("\nFetching WOs from:", remoteAusgang)
                wosDict = pf.getWOsFromRemoteAusgang(sftp, remoteAusgang, fetchCount=None)
                
                local_path = local_dir / "noProcedure"
                remoteEingang = f"{archiveRoot}/eingang/{cfg['eingang']['MAE']['MAE_IFTMIN_D95B']}"
                
                for filename in tqdm(
                    sftp.listdir(remoteEingang)
                    ,total=len(pf.listFilesRecursive(sftp, remote_dir=remoteEingang))
                ):
                    if filename.endswith(".arc"):
                        continue
                    ojn = "Unknown"
                    row = None   
                    ton = "Unknown"     
                    remote_path = remoteEingang + "/" + filename
                    
                    with sftp.open(remote_path, 'r') as remote_file:
                                
                        content = remote_file.read().decode(errors="ignore")
                        for line in content.replace("\n", "").replace("\r", "").split("'"):
                            
                            ojnMatch = ep.ojnPattern.search(line)
                            if ojnMatch:
                                ojn = ojnMatch.group(1)
                                
                            
                            if ep.woPattern.search(line):
                                ton = ep.woPattern.search(line).group(1)
                                
                            if ep.filenamePattern.search(line):
                                filename = ep.filenamePattern.search(line).group(1)
                                
                            if "BGM+" in line:
                                break
                            
                        if wosDict.get(ton, None):
                            ep.maeFetchCustomsProcedureOrders(sftp, content, remote_path, local_path, ton, filename, ojn)

            # if "commodityIssueBool" == cfg["sftpProcedures"][ioSelector]:
            #     print("\nStarting to analyze commodity weight inconsistencies...")
            #     remoteAusgang = f"{archiveRoot}/ausgang/{cfg['ausgang']['MAE']['MAE_IFTMIN_Commodities_Mail']}"
                
            #     print(f"Fetching OJNs from: {remoteAusgang}")
            #     ojnsDict = pf.getOJNsFromRemoteAusgang(sftp, remoteAusgang, fetchCount=None)

            #     remoteEingang = f"{archiveRoot}/eingang/{cfg['eingang']['MAE']['MAE_IFTMIN_D95B']}"
            #     for filename in sftp.listdir(remoteEingang):
            #         if filename.endswith(".arc"):
            #             continue
            #         row = None
            #         if filename.split(".")[0][5:] in data:
            #             remote_path = remoteEingang + "/" + filename
                        
                            
            #             with sftp.open(remote_path, 'r') as remote_file:
                                    
            #                 content = remote_file.read().decode(errors="ignore") 
                            
            #                 for line in content.split("\n"):
                                
            #                     match = re.search(r"'A8(\d+)'", line)
            #                     if match:
            #                         row = ep.maeCommodityWeightAnalysis(content)
            #                         if row:
            #                             dfData.append(row)
            #                         matchedOJNsList.append(match.group(1))
            #                         # print(content.decode())
            #                         continue
                
            #     print("Starting excel spreadsheet creation...")
            #     df = pd.DataFrame(dfData)

            #     # Ensure the column is numeric
            #     if "AccCommodityWeightSum" in df.columns:
            #         df["AccCommodityWeightSum"] = pd.to_numeric(df["AccCommodityWeightSum"], errors="coerce")
            #         df["AccCommodityWeightSum"] = df["AccCommodityWeightSum"].fillna(0)

            #     WeightsConsistentCount = len(df.loc[df["WeightsConsistent"] == True])
            #     weightsConsistentRatio = round(WeightsConsistentCount/len(df) * 100, 2)

            #     print(f"\nFound {len(matchedOJNsList)} of {len(data)} in {remoteEingang}")
            #     weightsConsistentRatioVerbose = f"WeightsConsistentRatio: {weightsConsistentRatio} % < {WeightsConsistentCount}/{len(matchedOJNsList)}"

            #     print(weightsConsistentRatioVerbose)
            #     reportNAME = f'Analysis_{len(matchedOJNsList)}of{len(data)}_EDIs_{dt.datetime.now().strftime("%F_%T")}.xlsx'

            #     # Define formatting rules
            #     formattingRules = {
            #         "WeightsConsistent": [
            #             {"type": "cell", "criteria": "==", "value": False, "format": {"bg_color": "red"}},
            #         ],
            #         "AccCommodityWeightSum-NetWeights in t": [
            #             {"type": "cell", "criteria": "<", "value": 0, "format": {"font_color": "red"}},
            #             {"type": "cell", "criteria": ">", "value": 0, "format": {"font_color": "blue"}},
            #         ],
            #     }

            #     ExcelWriter(root / "tmp" / reportNAME.replace(":", "-"), dfDict={f"Analysis of {len(matchedOJNsList)} of {len(data)}": df}, formattingRules=formattingRules)
            #     print(f"Excel spreadsheet saved: {root / 'tmp' / reportNAME.replace(':', '-')}")
            #     # df.to_excel(root / "tmp" / reportNAME.replace(":", "-"), index=False)

    # sftp.close()
    # transport.close()