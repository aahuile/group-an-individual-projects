#haven library installation for sas datasets
if(!require("haven")) install.packages("haven");library("haven")

#reading datasets (change path for different computers)
Analytic_dataset <- read_sas("C:/Users/abannem/Documents/MSc In Big Data For Business/Courses/9. Business Analytics Tools - Open Source/OpenSourceProgramming/Group Assignment/AnalyticDataInternetGambling.sas7bdat")
Demographics <- read_sas("C:/Users/abannem/Documents/MSc In Big Data For Business/Courses/9. Business Analytics Tools - Open Source/OpenSourceProgramming/Group Assignment/RawDataIDemographics.sas7bdat")
PokerChip <- read_sas("C:/Users/abannem/Documents/MSc In Big Data For Business/Courses/9. Business Analytics Tools - Open Source/OpenSourceProgramming/Group Assignment/RawDataIIIPokerChipConversions.sas7bdat")
DailyAggregation <-read_sas("C:/Users/abannem/Documents/MSc In Big Data For Business/Courses/9. Business Analytics Tools - Open Source/OpenSourceProgramming/Group Assignment/RawDataIIUserDailyAggregation.sas7bdat")

# View(PokerChip)

#Datamart building 

#dplyr, tidyr and data.table 
if(!require("dplyr")) install.packages("dplyr");
library("dplyr")
if(!require("tidyr")) install.packages("tidyr");
library("tidyr")

if(!require("data.table")) install.packages("data.table");
library("data.table")

#processing of pokerchip table
pokerchip2 <- PokerChip %>%
  group_by(UserID)%>%
  mutate(Total_Buy =length(which(TransType==124)),Total_Sell=length(which(TransType==24)))
# View(pokerchip2)


#filter by transtype124 and then take minimum maximum and mean
pokerchip3 <- pokerchip2 %>% 
  filter(TransType == 124)%>%
  group_by(UserID) %>% 
  mutate(Min_Buy= min(TransAmount),Max_Buy=max(TransAmount),Mean_Buy=mean(TransAmount),First_Buy_Date=min(TransDateTime),Last_Buy_Date=max(TransDateTime))  


#delete other columns from poker chip 3 
pokerchip3[,2:4]<-NULL

#remove duplicates 
pokerchip3 <-unique(pokerchip3)

#filter by transtype24 and then take minimum maximum and mean
pokerchip4 <- pokerchip2 %>% 
  filter(TransType == 24)%>%
  group_by(UserID) %>% 
  mutate(Min_Sell= min(TransAmount),Max_Sell=max(TransAmount),Mean_Sell=mean(TransAmount),First_Sell_Date=min(TransDateTime),Last_Sell_Date=max(TransDateTime))

#delete other rows from poker chip 4 and trans124 and trans24 to not have repeated columns 
pokerchip4[,2:6]<-NULL

#remove duplicates from poker chip 4
pokerchip4 <-unique(pokerchip4)

#join pokerchip3 and pokerchip4 into pokerchip final

pokerchipfinal <- merge(pokerchip3, pokerchip4, by = "UserID", all = TRUE)
# View(pokerchipfinal)

#Analytic_dataset and demographics have no repeated User ids but daily aggration
# has 
dim(Analytic_dataset[duplicated(Analytic_dataset$USERID),])[1]
dim(Demographics[duplicated(Demographics$UserID),])[1]
dim(DailyAggregation[duplicated(DailyAggregation$UserID),])[1]

#Daily aggregation processing 

#Adding up the total stakes 
daily1 <- data.table(DailyAggregation)
daily2 <- daily1[,list(Total_Stakes = sum(Stakes), freq = .N), by = "UserID"]
daily2[,3]<-NULL
#Adding total winnings
daily3 <- data.table(DailyAggregation)
daily4 <- daily3[,list(Total_Winnings = sum(Winnings), freq = .N), by = "UserID"]
daily4[,3]<-NULL
#Adding total bets 
daily5 <- data.table(DailyAggregation)
daily6 <- daily5[,list(Total_Bets = sum(Bets), freq = .N), by = "UserID"]
daily6[,3]<-NULL

#ocurrences of the products
View(DailyAggregation)

DailyAggregation_filter <- DailyAggregation %>%
  group_by(UserID)%>%
  mutate(Product1 =length(which(ProductID==1)),
         Product2 =length(which(ProductID==2)),
         Product3 =length(which(ProductID==3)),
         Product4 =length(which(ProductID==4)),
         Product5 =length(which(ProductID==5)),
         Product6 =length(which(ProductID==6)),
         Product7 =length(which(ProductID==7)),
         Product8 =length(which(ProductID==8)))

# View(DailyAggregation_filter)

#Remove extra columns
DailyAggregation_filter[,2:6] <- NULL

#remove duplicates
DailyAggregation_filter <-unique(DailyAggregation_filter)

#verify if there are no duplicates
dim(DailyAggregation_filter[duplicated(DailyAggregation_filter$UserID),])[1]

#Merge daily2, daily4, daily6 and Daily_Aggregation_filter to build Daily final
#daily2 and daily4 to do daily 7

daily7 <- merge(daily2, daily4, by = "UserID", all = TRUE)

#daily7 and daily 6 to do daily8 
daily8<- merge(daily7,daily6,by="UserID",all= TRUE)

#daily 8 and Daily_Aggregation_filter to do daily final

dailyfinal<- merge(daily8,DailyAggregation_filter,by="UserID",all=TRUE)

#Merge of all the final tables to have the final datamart (demographics as starting point )

#Demographics with dailyfinal

demo_daily <- merge(x = Demographics, y = dailyfinal, by = "UserID", all.x = TRUE)

# View(demo_daily)

#demo_daily with Analytic_dataset
#change primary key of Analytic dataset first 
colnames(Analytic_dataset)[1] <- "UserID"

#merge 
dd_analytics <- merge(x = demo_daily, y = Analytic_dataset, by = "UserID", all.x = TRUE)

#dd_analytics with pokerchipfinal

datamart <- merge(x = dd_analytics, y = pokerchipfinal, by = "UserID", all.x = TRUE)
View(datamart)

#verify that there are no repeated userID 
dim(datamart[duplicated(datamart$UserID),])[1]

#Delete duplicated columns 
colnames(datamart)

datamartFinal <- datamart[-c(4,24:25,28)]

colnames(datamartFinal)

#we fill the missing values in Age column with the mean of the column
datamartFinal[is.na(datamartFinal[,"AGE"]), "AGE"] <- round(mean(datamartFinal[,"AGE"], na.rm = TRUE),digits = 0)

# View(datamartFinal)

#Change NA to 0 for amount columns (An NA in this case represents a 0)
datamartFinal[, 38:42][is.na(datamartFinal[, 38:42])] <- 0


# Create and read excel with keys for countries, languages and applicationID
install.packages("readxl")
library(readxl)

Countries <- read_excel("C:/Users/abannem/Documents/MSc In Big Data For Business/Courses/9. Business Analytics Tools - Open Source/OpenSourceProgramming/Group Assignment/Countries.xlsx")
Languages<-read_excel("C:/Users/abannem/Documents/MSc In Big Data For Business/Courses/9. Business Analytics Tools - Open Source/OpenSourceProgramming/Group Assignment/Languages.xlsx")
Application_ID<-read_excel("C:/Users/abannem/Documents/MSc In Big Data For Business/Courses/9. Business Analytics Tools - Open Source/OpenSourceProgramming/Group Assignment/Application_ID.xlsx")

#Now we merge these datasets with their respective keys (Country = Countries, Language = languages, ApplicationID = ApplicationID)
datamartFinal <- merge(datamartFinal, Countries, by.x="Country", by.y="Country")
datamartFinal <-merge(datamartFinal, Languages, by.x="Language", by.y="Language")
datamartFinal <-merge(datamartFinal, Application_ID, by.x = "ApplicationID", by.y = "ApplicationID")

colnames(datamartFinal)

View(datamartFinal)

datamartFinal$Gender[datamartFinal$Gender == 1] <- 'Male'
datamartFinal$Gender[datamartFinal$Gender == 0] <- 'Female'

#################################### Shiny application ####################################

if (!require("devtools"))
  install.packages("devtools")
devtools::install_github("shiny", "rstudio")


library (ggplot2)
library(shiny)

shinyApp(
  ui = fluidPage(
    selectInput("bets", "Choose:",
                list(`On Total Bets Basis` = list("Bets by Language", "Bets by Country", "Bets by Application Description"),
                     `On Customers Basis ` = list("Customers by Language", "Customers by Country", "Customers by Product","Customers by Age"),
                     `On Gender Basis ` = list("Gender Repartition Countrywise","Gender Winnings"),
                     `On PokerChip Basis ` = list("PokerChips Bought by Country","PokerChips Sold by Country"))
    ),
    plotOutput("result")
  ),
  server = function(input, output) {
    output$result <- renderPlot({
      
      if (input$bets == "Bets by Language"){
        
        ggplot(data = datamartFinal, aes(Language_Description, Total_Bets)) +
          geom_bar(stat="identity") +
          theme(axis.text.x=element_text(angle=90,hjust=1,vjust=0.5)) +
          # Use the input value as the plot's title
          ggtitle(input$title)
      }
      
      else if (input$bets == "Bets by Country"){
        ggplot(data = datamartFinal, aes(CountryName, Total_Bets)) +
          geom_bar(stat="identity") +
          theme(axis.text.x=element_text(angle=90,hjust=1,vjust=0.5)) +
          # Use the input value as the plot's title
          ggtitle(input$title)
      }
      
      else if (input$bets == "Bets by Application Description"){
        ggplot(data = datamartFinal, aes(Application_Description, Total_Bets)) +
          geom_bar(stat="identity") +
          theme(axis.text.x=element_text(angle=90,hjust=1,vjust=0.5)) +
          # Use the input value as the plot's title
          ggtitle(input$title)
      }
      
      else if (input$bets == "Customers by Country"){
        ggplot(data = datamartFinal, aes(CountryName, UserID)) +
          geom_bar(stat="identity") +
          theme(axis.text.x=element_text(angle=90,hjust=1,vjust=0.5)) +
          # Use the input value as the plot's title
          ggtitle(input$title)
      }
      
      else if (input$bets == "Customers by Language"){
        ggplot(data = datamartFinal, aes(Language_Description, UserID)) +
          geom_bar(stat="identity") +
          theme(axis.text.x=element_text(angle=90,hjust=1,vjust=0.5)) +
          # Use the input value as the plot's title
          ggtitle(input$title)
      }
      
      else if (input$bets == "Customers by Product"){
        ggplot(data = DailyAggregation, aes(ProductID, UserID)) +
          geom_bar(stat="identity") +
          theme(axis.text.x=element_text(angle=90,hjust=1,vjust=0.5)) +
          # Use the input value as the plot's title
          ggtitle(input$title)
      }
      
      else if (input$bets == "Customers by Age"){
        ggplot(data = datamartFinal, aes(AGE, UserID)) +
          geom_bar(stat="identity") +
          theme(axis.text.x=element_text(angle=90,hjust=1,vjust=0.5)) +
          # Use the input value as the plot's title
          ggtitle(input$title)
      }
      
      else if (input$bets == "Gender Winnings"){
        ggplot(data = datamartFinal, aes(Gender, Total_Winnings)) +
          geom_bar(stat="identity") +
          theme(axis.text.x=element_text(angle=90,hjust=1,vjust=0.5)) +
          # Use the input value as the plot's title
          ggtitle(input$title)
      }
      
      else if (input$bets == "Gender Repartition Countrywise"){
        ggplot(data = datamartFinal) + 
          geom_density(aes(x=CountryName,fill=factor(Gender)),bins=10, position = "identity",alpha = 0.5, adjust=2)+
          theme(axis.text.x=element_text(angle=90,hjust=1,vjust=0.5)) +
          ggtitle(input$title)
      }
      
      else if (input$bets == "PokerChips Bought by Country"){
        ggplot(data = datamartFinal, aes(CountryName, Total_Buy)) +
          geom_bar(stat="identity") +
          theme(axis.text.x=element_text(angle=90,hjust=1,vjust=0.5)) +
          # Use the input value as the plot's title
          ggtitle(input$title)
      }
      
      else if (input$bets == "PokerChips Sold by Country"){
        ggplot(data = datamartFinal, aes(CountryName, Total_Sell)) +
          geom_bar(stat="identity") +
          theme(axis.text.x=element_text(angle=90,hjust=1,vjust=0.5)) +
          # Use the input value as the plot's title
          ggtitle(input$title)
      }
      
    })
  }
)

