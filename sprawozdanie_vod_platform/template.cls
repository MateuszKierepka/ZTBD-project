\NeedsTeXFormat{LaTeX2e}
\ProvidesClass{pktemplate}[2026/03/11 Klasa do sprawozdan laboratoryjnych]

\LoadClass[12pt,a4paper]{article}

\RequirePackage[utf8]{inputenc}
\RequirePackage{polski}
\RequirePackage[T1]{fontenc}
\RequirePackage{graphicx}
\RequirePackage{geometry}
\RequirePackage[hidelinks]{hyperref}
\RequirePackage{titlesec}
\RequirePackage{amsmath}
\RequirePackage{tabularx}
\RequirePackage{booktabs}
\RequirePackage{float}
\RequirePackage{caption}
\RequirePackage{listings}
\RequirePackage{titlesec}

\RequirePackage[dvipsnames]{xcolor}

% Pakiet do kodów i algorytmów
\RequirePackage{listings}
\RequirePackage[ruled,vlined,linesnumbered]{algorithm2e}

\titlespacing*{\section}{0pt}{0pt}{10pt}

% --- Definicje nazw (jeśli nie używasz zewnętrznych zmiennych \Lang...) ---
\newcommand{\LangAlgorithm}{Algorytm}
\newcommand{\LangListing}{Listing}
\newcommand{\LangListOfListings}{Spis listingów}

% --- Konfiguracja listings ---
\lstset{
    basicstyle=\ttfamily\footnotesize,
    backgroundcolor=\color{gray!5},
    commentstyle=\itshape\color{Green},
    keywordstyle=\color{Red},
    stringstyle=\color{Blue},
    numberstyle=\tiny\color{Black},    
    escapeinside={`}{'},
    frame=single,
    tabsize=2,
    rulecolor=\color{black!30},
    title=\lstname,
    breaklines=true,
    breakatwhitespace=true,
    framextopmargin=2pt,
    framexbottommargin=2pt,
    extendedchars=false,
    captionpos=b,
    abovecaptionskip=5pt,
    keepspaces=true,            
    numbers=left,                    
    numbersep=5pt,                  
    showspaces=false,                
    showstringspaces=false,
    showtabs=false
}

% --- Konfiguracja nazw i spisów ---
\renewcommand{\lstlistingname}{\LangListing}
\renewcommand{\lstlistlistingname}{\LangListOfListings}

% Jeśli używasz algorithm2e:
\SetAlgorithmName{\LangAlgorithm}{\LangAlgorithm}{Spis algorytmów}


\geometry{margin=1in}

\setlength{\parskip}{6pt}
\setlength{\parindent}{0pt}

\newcommand{\grupa}[1]{\def\@grupa{#1}}

\newcommand{\autorA}[1]{\def\@autorA{#1}}
\newcommand{\albumA}[1]{\def\@albumA{#1}}

\newcommand{\autorB}[1]{\def\@autorB{#1}}
\newcommand{\albumB}[1]{\def\@albumB{#1}}

\newcommand{\prowadzacyA}[1]{\def\@prowadzacyA{#1}}
\newcommand{\prowadzacyB}[1]{\def\@prowadzacyB{#1}}

\titleclass{\section}{top}

\renewcommand{\maketitle}{
    \begin{titlepage}
        \begin{center}
            \includegraphics[width=1\linewidth]{logopk.png}\\[1.5cm]
            
            {\huge \bfseries \@title} \\[2cm]
            
            \begin{flushright}
                \large
                Grupa \@grupa \\[0.5cm]
                \@autorA \\
                Nr albumu \@albumA \\[0.5cm]
                \@autorB \\
                Nr albumu \@albumB \\[3cm]

                Prowadzący: \\
                \@prowadzacyA \\
                \@prowadzacyB \\
                
                \vfill
                Kraków, \today
            \end{flushright}
        \end{center}
    \end{titlepage}
}