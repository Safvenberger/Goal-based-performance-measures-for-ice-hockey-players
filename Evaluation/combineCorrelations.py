#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Rasmus Säfvenberg

import os
import pandas as pd
import re

def combine_files(file: str) -> pd.DataFrame:
    """
    Combine correlation files created in Python (Pearson/Spearman) with those
    created in R (MIC).

    Parameters
    ----------
    file : str
        Path to file containing correlation information as created by 
        correlations.py or MIC.R.

    Returns
    -------
    corr_df : pd.DataFrame
        Data frame with all evaluation metrics for all partitions considered.

    """
    # Pearson and spearman correlations
    pear_spear = pd.read_csv(f"../Results/{file}")
    
    # Find the MIC version of the same file
    mic_equiv = file.replace("corr", "mic")

    # MIC values
    mic = pd.read_csv(f"../Results/{mic_equiv}")
    
    # Combine the information about correlations and MIC
    corr_df = pear_spear.merge(mic, on = ["Season", "Metric", 
                                          "PartitionSize", "Part"])
    
    # Reorder the columns
    corr_df = corr_df[["Season", "Metric", "PartitionSize", "Part", 
                       "Pearson", "Spearman", "MIC"]]
    
    return corr_df


def corr_to_excel(file_name: str="correlations"):
    """
    Create an .xlsx file containing correlations (including MIC) for all 
    partitions created.

    Parameters
    ----------
    file_name : str, default is "correlations"
        The name of the excel file to create/overwrite.
    Returns
    -------
    None. Instead, the results are saved in .xlsx files.

    """
    # Get all results files
    files = os.listdir("../Results")
    
    # Keep only the files regarding correlation
    files = [corr_file for corr_file in files if (corr_file.startswith("corr_") or
             corr_file.startswith("mic_")) and corr_file.endswith(".csv")]
    
    # Create a xlsxwriter object
    with pd.ExcelWriter(f"../Results/{file_name}.xlsx") as writer:
        for file in files:
            if file.startswith("corr_"):
                # Combine the correlation and MIC file
                corr_df = combine_files(file)
                
                # Create the name of the sheet
                sheet_name = re.sub("corr_|.csv", "", file)
                
                # Replace trad with traditional
                sheet_name = re.sub("trad", "traditional", sheet_name)
                
                # Replace mult with multiple
                sheet_name = re.sub("mult", "multiple", sheet_name)
                
                # Replace reg with regular season
                sheet_name = re.sub("reg", "regular season", sheet_name)
                
                # Replace play with playoffs
                sheet_name = re.sub("play$", "playoffs", sheet_name)
                
                # Remove leading underscores
                sheet_name = re.sub("^_", "", sheet_name)
                
                # Replace underscore with space
                sheet_name = re.sub("_", " ", sheet_name)
                
                # Fix capitalization
                sheet_name = sheet_name[:1].upper() + sheet_name[1:]

                # Write to the excel file
                corr_df.to_excel(writer, f"{sheet_name}", index=False)  
        
                # Get the xlsxwriter workbook and worksheet objects.
                workbook  = writer.book
                worksheet = writer.sheets[f"{sheet_name}"]
                
                # Number formatting
                format_num = workbook.add_format({'num_format': '#,##0.000'})

                # Specify column width for column C
                worksheet.set_column(1, 1, 12)
                
                # Specify column width for column C
                worksheet.set_column(2, 2, 12)
                
                # Specify column width for column E-G
                worksheet.set_column(4, 6, 10, format_num)
                
        
            
if __name__ == "__main__":
    corr_to_excel()

