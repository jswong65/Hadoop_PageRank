# Hadoop_PageRank
Hadoop implementation for PageRank

Two common issues:
* **Dead Ends**: The network contains one or more webpage that has no outbound links to other webpages. ⇒ The weight will be vanished eventually.
* **Spider Trap**: The network contains one or more webpage that has the only outbound link which connects to itself. ⇒ Those webpages will have extremely high PageRank Score.

Solution: 
* Teleporting is the proposed solution to alleviate the issues by allowing webpages randomly connect to  all other pages ⇒ A certain proportion (Beta) of the PR(T - 1) will be evenly distributed to all of the webpages. 
* PR(T) = (1 - Beta) * PR(T - 1) * Transition Matrix + Beta * PR(T - 1)

### Inputs
Input1: Adjacent list of webpages (relations.txt)
* format: fromPage\t toPage1,toPage2,toPage3
* 1 (From)	5,7,9 (To)

Input2: Initial Pagerank value of each webpage (All of the values are initialized as 1 / N, such that N equals to the total number of webpages) (pr.txt)
* format: Page\t PageRank
* 1	0.012

### MapReduce Components:
1. UnitMultiplication:
    * TransitionMapper: Builds the transition matrix 
      *  creates <FromPage, To:Transition_Probability(From⇒To)> pairs 
      *  E.g., 1 (From)	5,7,9 (To) ⇒ <1,5:1/3>, <1,7:1/3>, <1,9:1/3>
    * PRMapper: Calculates PR * (1 - Beta)
      *  Creates <FromPage, PR(FromPage) * (1 - Beta)> pairs
    * MultiplicationReducer: Obtains the contribution of FromPage to ToPage 
      *  Write out <To, UnitMultiplication>
      *  UnitMultiplication = PR(FromPage) * (1 - Beta) * Transition_Probability (From⇒To)
2. UnitSum:
    * PassMapper
    * BetaMapper
    * SumReducer
