# Nemo Financial Modelling Framework
This is a project that I built for myself in order to learn about how financial modelling tools work, specifically in the context of life insurance. 

Modelling software packages in this world are generally based on Columns and Scalars. They project cashflows, risks, and other business metrics into the future. 

They are used for SII reporting, reserving, scenario stress testing, IFRS17 reporting and more. However they are general purpose so they can be used for almsot anything

Some similar software packages include
 - WTW: RiskAgilityFM
 - FIS: Prophet
 - Milliman: MG ALFA

This software is intended to perform a similar function to the above, but with a simplifed feature set. It may also function as a learning aid, for those looking to understand how these calculations are done in other software under the hood. It will most closely resemble RiskAgility FM (as that is the one I am most familiar with), but will have similarities with all of those software packages.

If you wish to use this in production, you do so at your own risk. This software has not been independently tested, audited or validated. (as of 29/12/2024)

## Philosophy
I've worked with several of the software packages mentioned above. Whilst each of them has their own strengths, it is my opinion that they all share the same weaknesses. Namely:
1. Feature bloat/Legacy feature support
2. Lack of source code transparency 
3. Inability to modify the source code to introduce custom behavior (or fix bugs)
4. In some cases, performance limitations
5. Overall a greater emphasis on making the software marketable rather than making it good

Point 5 is a big one. Most commercial softwares have heavily locked down interfaces that are designed to look good to non-technical executives making purchasing decisions. 
This  comes at the cost of functionality to the actual users of the software. Many of them even encrypt the underlying source files so that the only way to change anything is though their (usually pretty bad) interface

There is also a great deal of care taken to make it as difficult as humanly possible to export your model or customise its behaviour. This is done so they can make it expensive and time consuming to switch and helps them 'lock in' their customer base, or to reduce support tickets. But to me this is scumbag behaviour and I won't have it on anything I build. My design philosphy here is the precise oposite of that. 
You are free to play with, edit, modify, customise, break, import, export, and integrate things however you wish. Note that support options are also virtually non-existent. But the project is simple enough that that shouldnt be a problem for a competent user

This software doesnt have a UI. It doesnt need one. Visual Studio or Rider is the best interface you could ever have for building and debugging fnancial models. Attempting to build a UI to replace is a huge overhead, an enormous source of complexity, and an additional layer of obfuscation that you just don't need. 
If you want to build an interface as part of your particular model, you are completely free to do so. But that is a model level feature, not a framework level feature

And lastly, modelling is programming. To be able to model you must be able to program. If, as a software provider, your value proposition to a customer is that your software lets people model without understanding what they are doing, then you have failed before you have even started. And you should probably not designing software at all. 
That doesnt mean every user needs to understanding everything about it, but it does mean that at least someone on your staff needs to. 

Trying to avoid this will lead to slow, expensive, inflexible, unreadable, unmaintainable and and generally inefficient models and there is no magic solution to this despite what all the software providers may promise you. They are all wrong. Yes all of them
![Alt text](https://i.kym-cdn.com/photos/images/original/002/368/468/ba1.jpg "a title")

(You dont have to agree with me to use the software 😉)

### Disclaimer
This was built entirely on my own device and in my own time. It is not based on any other software nor has any proprietary code been used.
Whilst some general features of financial modelling tools might be similar (columns, scalars etc), the implimentation details are entirely my own. 

This project currently these dependencies
- CLOSEDXML - Distributed under the MIT license. Source: https://github.com/ClosedXML/ClosedXML?tab=readme-ov-file
- SEP - Distributed under the MIT license. Source: https://github.com/nietras/Sep?tab=readme-ov-file


## Now that thats out of the way...
Lets take a look at the Example Model. Before we continue, I know that the calculations in this model arent very acurate. It doesnt matter. Its for demonstration purposes only

To define a new model, we need to create a new class which inherits from Nemo.Model.ModelBase

### ModelBase
ModelBase is a base class that lets the model interact with the engine. It also handles a lot of boiler plate for you and do some setup with the inbuilt output buffer systems

- Provides a way for you to map fields from your input file to scalars in the model. MapDataToScalar
- Provides a virtual default Target implimentation that uses reflection to identify all of your model columns at runtime, and calculate all of them
- Initialises the output buffers based on the OutputSet
- Handles copying data from the columns and scalars into the output buffer at the end of each record's caclculations
- Provides a handful of virtual methods which are invoked at specific points in the calculation loop to enable custom behaviour

Your model class can then do whatever the hell you want it to do. If you want it to run pacman you can. But the intention by default is that you would add a series of columns and scalars to your class to perform your projections

For further details read the source code for this class, and see how the API works by viewing the ExampleModel

### Columns
A column is essentially a wrapper around a function, and a buffer of results which act to CACHE the values for a calculation. the cache is only cleared when the model is reset, and that happens when the current input record changes.

So column values are calculated only once <b>per time period, per policy</b>

Additional calls to the same value from other calculations will retrieved a cached value. 

The minimum and maximum time values permitted in the projection are thus required to be specified in advance.

### Projection

A projection is simply a collection of settings that determine what the model will calculate. In Nemo, at present the only settings included are
- T_Min
- T_Max
- T_Start
- T_End

Note that this must hold: 

    T_Min <= T_Start <= T_End <= T_Max

T_Min is the lowest value of t permitted in the projection. And T_Max is the largest value of t permitted_. The size of the columns internal buffer is T_Max - T_min + 1. 

Any column attempts to calculate outside this range it will throw an exception

T_Start is the first value of t that the model explicitly attempts to calculate by default. Up to and including T_End. 

In other words, the default implimenation for ModelBase.Target() is this:

    ChildModels.ForEach(x => x.Target()); //Force child calculations first
    foreach (var columnField in ColumnFields)
    {
        Column column = (Column)columnField.GetValue(this)!;
        for (var i = Context.Projection.T_Start; i <= Context.Projection.T_End; i++) 
        { 
            column.At(i); 
        }
    }

Note this calculates all time periods for a single column together. In some models it may be more appropriate to do all columns at each timestep. E.g.

    ChildModels.ForEach(x => x.Target()); //Force child calculations first
    for (var i = Context.Projection.T_Start; i <= Context.Projection.T_End; i++) 
    {     
        foreach (var columnField in ColumnFields)
        {
            Column column = (Column)columnField.GetValue(this)!;
            column.At(i);         
        }
    }

You can change this by overriding the Target() method in your Model class

### OutputSet
This is a little container for holding information about what will be output by the model

At present its just a list of Column and Scalar OutputNames. You can use the special case values "Scalars.All" or "Columns.All" to include everything. This is also the default behaviour.
I might add time filters in future but for now i havent bothered.

I decided to keep things simple and permit only columns in the aggregate output and Scalars only in the Individual output. Other software packages might provide a way of mixing and matchning but i dont believe this is necesary. If you want to include a column in the individual output, add a scalar to act as an interface, and have it simply read the column value you are interested in

### Model Context

This is a container that holds everything relevent for the current model job. That includes the Projection, OutputSet and the SourceManager as well as a name and a directory for the job

This is passed into the constructor of Columns and Models to pass the relevant information around

## Engine

The engine class is essentially a glorified for loop. But it handles some additional stuff like multithreading, chunking, initialising some components and making sure all the events are called in the correct sequence. It is also the owner of the output buffers for each output group

This class is best understood by reading the source code directly.

Examples for how to use the API for the engine can be found in the ExampleModel class

### Other
There aree loads of things thise readme doesnt cover. Like this source manager, or the csv reader.  These are complicated and i havent yet written any documntaion for them but i will get there eventually. Feel free to read the source to understand more

There are also scalars, shared columns, or the Table class and its lookups. These arent that complicated and im sure you can infer what you need from the source or the example model

### Contributions

Whilst I dont really expect this to go anywhere, if you have any suggestions feel free to contribute or leave suggestions. 
