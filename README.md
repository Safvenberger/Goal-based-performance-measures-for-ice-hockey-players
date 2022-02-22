# Goal-based performance measures for ice hockey players

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) ![R](https://img.shields.io/badge/r-%23276DC3.svg?style=for-the-badge&logo=r&logoColor=white) ![MySQL](https://img.shields.io/badge/MySQL-005C84?style=for-the-badge&logo=mysql&logoColor=white)

## Description

In ice hockey, traditional metrics regarding goals, assists, points and plus-minus are typically used to rate and rank players. However, this does not always account for the importance of contribution to the goal itself. For instance, a goal scored in a tie-game with less than a minute left is more likely to be important for the outcome of the game than a goal scored in an empty net while leading by 5+ goals. Thus, in this project a weighted metric known as the `Game Points Importance Value` (GPIV) is introduced. First, the probability of a given outcome given a specific context is

<img src="https://latex.codecogs.com/svg.image?P(Outcome&space;|&space;Context)&space;=&space;\frac{Occ(Context&space;|&space;Outcome)}{Occ(Context)}" title="P(Outcome | Context) = \frac{Occ(Context | Outcome)}{Occ(Context)}" />

where outcome is either win, tie or loss. The outcomes award 2, 1 and 0 points respectively. Occ is an acronym for occurrences and describes how often a specific context has occurred. The context is in turn defined by the time (in seconds), goal differential and manpower differential. The GPIV value is then calculated as

<img src="https://latex.codecogs.com/svg.image?GPIV(Context)&space;=&space;2&space;\cdot&space;[P(Win&space;|&space;ContextAG)&space;-&space;P(Win&space;|&space;ContextBG|)]&space;&plus;&space;1&space;\cdot&space;[P(Tie|&space;ContextAG)&space;-&space;P(Tie|&space;ContextBG|)]" title="GPIV(Context) = 2 \cdot [P(Win | ContextAG) - P(Win | ContextBG|)] + 1 \cdot [P(Tie| ContextAG) - P(Tie| ContextBG|)]" />

where BG and AG refer to the context before and after a goal respectively. 

## Instruction

### Data
The basis for this study is the NHL play-by-play data presented by Routley, K. & Schulte, O. in their paper _A Markov Game Model for Valuing Player Actions in Ice Hockey_. The data is available [here](https://www2.cs.sfu.ca/~oschulte/sports/)

### Repository structure

This project contains the most up-to-date version of the code. The structure is the following:
- **Scripts**: contains the Python scripts to count the occurrences, calculate GPIV and apply to each player in order to rank them accordingly.
- **Evaluation**: contains the Python and R scripts used to evaluate the correlation between various combinations of metrics.
- **Results**: contains the results as output by the relevant scripts.
- **Data**: contains additional data-files that might be needed. 

## Usage

The following steps illustrates how to obtain the results:

1. Download the data (in SQL format) as described in the previous section. Set-up a local MySQL server containing the data with the schema name `hockey` with username `root` and password `password`.
2. Run the function _apply_weighted_reward()_ defined in the script `weighted_reward.py` to obtain the GPIV metrics for a given time-period of interest. Use appropriate arguments to analyze the data of interest as well as to avoid overwriting previous results. Note that the subsetting of games is done through the use of dates in the integer representation "yyyymmdd" and thus requires these dates to exist. If they do not already exist, these dates will be scraped from [hockey-reference.com](https://www.hockey-reference.com).
3. (Optional) Evaluate the results by running the evaluation scripts `correlations.py` and `MIC.R`. These results can then be combined by running `combineCorrelations.py`.
4. Get the ranking of players by running the script `getPlayerRankings.py`. The corresponding results will be saved in an .xslx file and stored in the folder Results.

## Credits

Based on the master thesis and code written by Jon Vik. Supervision by [Patrick Lambrix](https://www.ida.liu.se/~patla00/) and [Niklas Carlsson](https://www.ida.liu.se/~nikca89/).

## Links
- [Sports Analytics @ LiU](https://www.ida.liu.se/research/sportsanalytics/)
- [Not all goals are equally important - a study for the NHL](https://www.ida.liu.se/~patla00/publications/MathSport2021-extended.pdf)

