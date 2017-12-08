# Load packages ----
library(shiny)
# Loading quantmod - an R package which
library(quantmod)

# Loading the helpers.R script which contains a function adjust
# which allows for adjusting stock prices to inflation
source("helpers.R")

# User interface ----
ui <- fluidPage(
  titlePanel("stockVis"),
  
  sidebarLayout(
    sidebarPanel(
      helpText("Select a stock to examine. 
        Information will be collected from Google finance."),
# text box for entering the name(symbol) of the company
# whose stock share price we are interested in.
      textInput("symb", "Symbol", "SPY"),
# a date box, which allows user to choose a specific time period
# for the price of shares we are interested in
      dateRangeInput("dates", 
                     "Date range",
                     start = "2013-01-01", 
                     end = as.character(Sys.Date())),
 # empty horizontal lines     
      br(),
      br(),
# creating a checkbox to allow user to choose
# whether the prices of stock should be displayed on log scale
      checkboxInput("log", "Plot y axis on log scale", 
                    value = FALSE),
# creating a check box to allow users to choose whether
# the prices shown are adjusted to inflation
      checkboxInput("adjust", 
                    "Adjust prices for inflation", value = FALSE)
    ),
    
    mainPanel(plotOutput("plot"))
  )
)

# Server logic
server <- function(input, output) {

# reactive function to display results which are saved during 
# initial run of the app and are only re-computed if
# user changes the value of a widget, on which these results depend  
  dataInput <- reactive({
    getSymbols(input$symb, src = "google", 
               from = input$dates[1],
               to = input$dates[2],
               auto.assign = FALSE)
    
  })

# We put the adjust function in a separate reactive,
# so that the adjusted prices will be recalculated only
# when the adjust box is checked/unchecked. If it
# is put inside the renderPlot and the adjust box is checked,  
# the prices will be
# recalculated every time the log box changes value.  
  adjustInput <- reactive({
    if (input$adjust == FALSE) return(dataInput())
    adjust(dataInput())
  })
  
  
 # renderPlot displays the output, based on the input from the widgets 
  output$plot <- renderPlot({
    
    chartSeries(adjustInput(),theme = chartTheme("white"), 
                type = "line", log.scale = input$log, TA = NULL)
  })
  
}

# Run the app
shinyApp(ui, server)
