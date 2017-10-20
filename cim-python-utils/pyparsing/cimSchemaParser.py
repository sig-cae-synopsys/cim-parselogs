# pg_dump_schemaparser.py
# parse schema.sql or pg_dump schema output file and create schema object
#
import sys
import json
#import pyparsing; print pyparsing.__version__
from pyparsing import Literal, CaselessLiteral, Word, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, upcaseTokens, Suppress, OneOrMore,\
    CharsNotIn,StringEnd
    
#
#    schema.sql statements grammar
#
SET = Keyword("set", caseless=True).addParseAction(upcaseTokens)
CREATE = Keyword("create", caseless=True).addParseAction(upcaseTokens)
CREATEFUNCTION = Keyword("create function", caseless=True).addParseAction(upcaseTokens)
CREATEORREPLACEFUNCTION = Keyword("create or replace function", caseless=True).addParseAction(upcaseTokens)
ALTERFUNCTION = Keyword("alter function", caseless=True).addParseAction(upcaseTokens)
DROPFUNCTION = Keyword("drop function", caseless=True).addParseAction(upcaseTokens)
COMMIT = Keyword("commit", caseless=True).addParseAction(upcaseTokens)
ALTER = Keyword("alter", caseless=True).addParseAction(upcaseTokens)
TABLE = Keyword("table", caseless=True).addParseAction(upcaseTokens)
EXTENSION = Keyword("extension", caseless=True).addParseAction(upcaseTokens)
COMMENTONEXTENSION = Keyword("comment on extension", caseless=True).addParseAction(upcaseTokens)
ONLY = Keyword("only", caseless=True).addParseAction(upcaseTokens)
INDEX = Keyword("index", caseless=True).addParseAction(upcaseTokens)
SEQUENCE = Keyword("sequence", caseless=True).addParseAction(upcaseTokens)
INSERT = Keyword("insert into", caseless=True).addParseAction(upcaseTokens)
VALUES = Keyword("values", caseless=True).addParseAction(upcaseTokens)
UPDATE = Keyword("update", caseless=True).addParseAction(upcaseTokens)
WHERE = Keyword("where", caseless=True).addParseAction(upcaseTokens)
PRIMARY = Keyword("primary key", caseless=True).addParseAction(upcaseTokens)
ADDCONSTRAINT = Keyword("add constraint", caseless=True).addParseAction(upcaseTokens)
CONSTRAINT = Keyword("constraint", caseless=True).addParseAction(upcaseTokens)
FOREIGN = Keyword("foreign key", caseless=True).addParseAction(upcaseTokens)
UPDATE = Keyword("update", caseless=True).addParseAction(upcaseTokens)
UNIQUE = Keyword("unique", caseless=True).addParseAction(upcaseTokens)
DEFAULT = Keyword("default", caseless=True).addParseAction(upcaseTokens)
TRUE = Keyword("true", caseless=True).addParseAction(upcaseTokens)
FALSE = Keyword("false", caseless=True).addParseAction(upcaseTokens)
NOTNULL = Keyword("not null", caseless=True).addParseAction(upcaseTokens)
ISNULL = Keyword("is null", caseless=True).addParseAction(upcaseTokens)
NULL = Keyword("null", caseless=True).addParseAction(upcaseTokens)
CHECK = Keyword("check", caseless=True).addParseAction(upcaseTokens)
REFERENCES = Keyword("references", caseless=True).addParseAction(upcaseTokens)
ON = Keyword("on", caseless=True).addParseAction(upcaseTokens)
USING = Keyword("USING btree", caseless=True).addParseAction(upcaseTokens)
OR = Keyword("or", caseless=True).addParseAction(upcaseTokens)
AND = Keyword("and", caseless=True).addParseAction(upcaseTokens)
STARTWITH = Keyword("start with", caseless=True).addParseAction(upcaseTokens)
START = Keyword("start", caseless=True).addParseAction(upcaseTokens)
INCREMENT = Keyword("increment", caseless=True).addParseAction(upcaseTokens)
OWNERTO = Keyword("owner to", caseless=True).addParseAction(upcaseTokens)
REVOKE = Keyword("revoke", caseless=True).addParseAction(upcaseTokens)
GRANT = Keyword("grant", caseless=True).addParseAction(upcaseTokens)

SEMICOLON  = Suppress(";")
LPAREN= Suppress("(")
RPAREN= Suppress(")")

ident = Word( alphas, alphanums + "_" ) #.addParseAction(lowercaseTokens) #.setName("identifier")
num = Word(nums)
setrvalue=ident | quotedString | num

#sqlComment = Suppress(Group( "--" + restOfLine ))    
sqlComment = Group( "--" + restOfLine )    
setStmt = Group( SET+ ident +  Group(CharsNotIn(";")) + SEMICOLON ) 
createExtStmt = Suppress(Group( CREATE + EXTENSION + Group(CharsNotIn(";")) +  SEMICOLON ))
commitStmt = COMMIT + SEMICOLON 
dropFuncStmt = Suppress(Group( DROPFUNCTION+ Group(CharsNotIn(";")) +  SEMICOLON ))
alterFuncStmt = Suppress(Group( ALTERFUNCTION+ Group(CharsNotIn(";")) +  SEMICOLON ))
createFuncStmt = Suppress(Group( (CREATEORREPLACEFUNCTION|CREATEFUNCTION) + Group(CharsNotIn(";")) +  SEMICOLON ))
commentExtStmt = Suppress(Group( COMMENTONEXTENSION + Group(CharsNotIn(";")) +  SEMICOLON ))
createTableStmt = Group( CREATE + TABLE + ident +  Group(CharsNotIn(";")) +  SEMICOLON )
#CREATE INDEX activemq_acks_xidx ON activemq_acks USING btree (xid);
createindexStmt= Group( CREATE + Optional(Suppress(UNIQUE)) + INDEX + ident +  Group(CharsNotIn(";")) +  SEMICOLON )
createsequenceStmt= Group( CREATE + SEQUENCE + Group(CharsNotIn(";")) +  SEMICOLON )
alterStmt = Group( ALTER + TABLE +Optional(Suppress(ONLY)) +ident +  Group(CharsNotIn(";")) +  SEMICOLON )
insertStmt = Group( INSERT +  ident +  Group(CharsNotIn(";")) +  SEMICOLON )
updateStmt = Group( UPDATE + ident +  Group(CharsNotIn(";")) +  SEMICOLON )
revokeStmt = Group( REVOKE + ident +  Group(CharsNotIn(";")) +  SEMICOLON )
grantStmt = Group( GRANT + ident +  Group(CharsNotIn(";")) +  SEMICOLON )
miscStmt = Suppress(Group(oneOf('begin return end $$', caseless=True)) +  Group(ZeroOrMore(CharsNotIn(";"))) +  SEMICOLON )
schemaSQL = OneOrMore(sqlComment | miscStmt | commitStmt |alterFuncStmt | dropFuncStmt | createFuncStmt | setStmt |createExtStmt |commentExtStmt | createTableStmt | alterStmt | insertStmt | updateStmt | createindexStmt | createsequenceStmt |revokeStmt |grantStmt)  + StringEnd()

fkspeccol = FOREIGN + LPAREN + Group(delimitedList(ident)) + RPAREN + REFERENCES + ident+LPAREN + Group(delimitedList(ident)) + RPAREN 
fkspeccol1 = CONSTRAINT + ident + PRIMARY +LPAREN + Group(delimitedList(ident)) + RPAREN 
pkeyspec = PRIMARY + LPAREN + Group(delimitedList(ident)) + RPAREN
uniquespec = UNIQUE + LPAREN + Group(delimitedList(ident)) + RPAREN


#check ((cim_license_id is null) <> (license_id is null))
#checkexp = ident + ( ISNULL | NOTNULL |  Word('<=>') + (num | LPAREN + Group(ident + Word('<=>') + quotedString ) + RPAREN) )
checkexpi = Group( ident + ( ISNULL | NOTNULL |  Word('<=>') + (num | LPAREN + Group(ident + Word('<=>') + quotedString ) + RPAREN) ))
checkexp = checkexpi | LPAREN + checkexpi + RPAREN
checkspec = CHECK + LPAREN + Group(checkexp + ZeroOrMore(( AND | OR | Literal('<>') ) + checkexp)) + RPAREN

#    CONSTRAINT summary_metric_contributor_multiplier_check CHECK ((multiplier >= (0)::double precision))
doublespec = Combine(Literal('(')+num+Literal(')')+Literal('::double precision'))
checksimple = Group(ident + ( ISNULL | NOTNULL |  Word('<=>') + (doublespec | num | quotedString)))
checkandor =  Group(LPAREN + checksimple + RPAREN + ZeroOrMore(( AND | OR | Literal('<>')) + LPAREN +  checksimple) + RPAREN)
textquali = Suppress(Literal('::text'))
textfield = LPAREN+ident+RPAREN+textquali
textquote = quotedString+textquali
checkcompare = Group(ident + Word('=') + Group(LPAREN + textfield + Word('=') + textquote +RPAREN) )
checkexp1 = checksimple | checkandor | checkcompare
constraintspec1 = CONSTRAINT + ident + CHECK + LPAREN + LPAREN + checkexp1 + RPAREN + RPAREN

timestamp_wo_tz = Keyword("timestamp without time zone", caseless=True) 
doubleprec =  Keyword("double precision", caseless=True)
basictype = oneOf('int int8 int4 int2 float8 oid date boolean bool text[] text timestamp bytea integer bigint smallint json name', caseless=True)
sizedtype = oneOf('char character varchar numeric', caseless=True) + LPAREN + Group(delimitedList(Word(nums))) + RPAREN
charactervarying = Keyword("character varying", caseless=True) + LPAREN + Group(Word(nums)) + RPAREN
textarray = Keyword("text ARRAY", caseless=True)
typespec = textarray | timestamp_wo_tz | doubleprec | charactervarying | basictype | sizedtype 
         
constraintspec = NOTNULL | NULL | UNIQUE | DEFAULT + (TRUE | FALSE | num)  | checkspec
colspec = (ident | quotedString )+ Group(typespec) + Group(ZeroOrMore(Group(constraintspec)))
columnspec = fkspeccol1 |fkspeccol | pkeyspec |  constraintspec1 | checkspec | uniquespec |  colspec

fieldspec = LPAREN + Group(delimitedList(ident))+ RPAREN
foreignkeyspec = FOREIGN + fieldspec + REFERENCES + ident
primarykeyspec = PRIMARY + fieldspec
uniquecspec = UNIQUE + fieldspec
addconstraintspec= ADDCONSTRAINT + ident + (foreignkeyspec | primarykeyspec | uniquecspec)
fkspec = ADDCONSTRAINT + ident + FOREIGN + LPAREN + ident+ RPAREN + REFERENCES + ident

#lower((uuid)::text)
idxfield = Combine(Literal("lower")+Literal('(')+ident+Literal(')')) | Combine(Literal("lower")+Literal('((')+ident+Literal(')')+Literal('::text')+Literal(')')) | ident 
idxspec = ON + ident + Optional(Suppress(USING)) +LPAREN + Group(delimitedList(idxfield)) + RPAREN
seqspec = ident + Optional((STARTWITH | START) + num) + Optional( INCREMENT + num)
tablespec= LPAREN + Group(delimitedList(Group(columnspec))) + RPAREN

constraints=[] #UNIQUE, CHECK (expression)
pkeys=[] #PRIMARY KEY 
tables=[] #CREATE TABLE
fkeys=[] #FOREIGN KEY
indices=[] #CREATE INDEX
sequences=[] #CREATE SEQUENCE

def test(parsExpr,s):
    print "---Test for '{0}'".format(s)
    try:
        result = parsExpr.parseString(s)
        print "  Matches: {0}".format(result)
    except ParseException as x:
        print "  No Match: {0}".format(str(x))
          
def parse_schema (f):
    print "Schema file",sys.argv[1]
    try:
        result = schemaSQL.parseFile(f) #,parseAll=True)
        print "Match Statements: ",len(result)
        '''
        print
        print "Last 3 parsed statements:"
        print result[len(result)-3]
        print result[len(result)-2]
        print result[len(result)-1]
        print
        '''
        process_statements (result)
    except ParseException as x:
        print "No Match: {0}".format(str(x))
        
def process_statements (r):
    for stmt in r:
        alters = 0
        if (stmt[0] == 'ALTER') and ( stmt[3][0] != ' OWNER TO coverity' ) :
            table_name=stmt[2].lower()
            alters += 1
            try:
                result = addconstraintspec.parseString(stmt[3][0])
                cname=result[1].lower()
                ctype=result[2]
                fields=result[3].asList()
                if ctype == 'UNIQUE':
                    constraints.append([table_name,cname,ctype,fields])
                else:
                    if ctype == 'PRIMARY KEY':
                        cname=table_name+'_pkey'
                        pkeys.append([table_name,cname,fields])
                        #print result
                        #print table_name,cname,fields
                    else:
                        if ctype == 'FOREIGN KEY':
                            reftable = result[5]
                            fkeys.append([table_name,[fields,reftable,['id']]])
            except ParseException as x:
                print "  No Match ALTER TABLE: {0}".format(str(x)), stmt[3]
        if stmt[0] == 'CREATE' :
            if stmt[1] == 'INDEX' :
                try:
                    result = idxspec.parseString(stmt[3][0])
                    indices.append([stmt[2].lower() , result[1] , len(result[2]) , result[2].asList()])
                except ParseException as x:
                    print "  No Match CREATE INDEX: {0}".format(str(x)), stmt
            if stmt[1] == 'TABLE' :
                table_name=stmt[2].lower()
                field_spec=[]
                #print '--------------------',table_name
                try:
                    result = tablespec.parseString(stmt[3][0])
                    for cs in result[0]:
                        if cs[0] in('PRIMARY KEY','FOREIGN KEY','UNIQUE','CHECK','CONSTRAINT'):                            
                            if cs[0] == 'PRIMARY KEY':
                                cname=table_name+'_'+'_pkey'
                                pkeys.append([table_name,cname,cs[1].asList()])
                            if cs[0] == 'FOREIGN KEY':
                                fkeys.append([table_name,cs[1].asList(),cs[3],cs[4].asList()])
                            if cs[0] == 'UNIQUE':
                                field_name= cs[1][0]
                                cname=table_name+'_'+field_name+'_key'
                                ctype=cs[0]
                                fields=cs[1].asList()
                                constraints.append([table_name,cname,ctype,fields])
                            if cs[0] == 'CHECK':
                                cexpr=cs[1].asList()
                                cname=table_name+'_'+cexpr[0][0]+'_check'
                                ctype=cs[0]
                                #print cs
                                constraints.append([table_name,cname,ctype,cexpr])
                                #print table_name,cname,ctype,cexpr
                            if cs[0] == 'CONSTRAINT':
                                cname=cs[1]
                                ctype=cs[2]
                                cexpr=cs[3].asList()
                                if ctype == 'PRIMARY KEY':
                                    pkeys.append([table_name,cname,cexpr])
                                else:
                                    constraints.append([table_name,cname,ctype,cexpr])
                        else:
                                field_name= cs[0]
                                type_spec=cs[1].asList()
                                constr_spec=cs[2].asList()
                                field_spec.append([field_name,type_spec,constr_spec])
                                for fc in constr_spec:
                                    ctype=fc[0]
                                    if ctype == 'CHECK':
                                        cname= table_name+'_'+field_name+'_check'
                                        cexpr= fc[1]
                                        constraints.append([table_name,cname,ctype,cexpr])
                                    if ctype == 'UNIQUE':
                                        cname = table_name+'_'+field_name+'_key'
                                        fields = [field_name]
                                        constraints.append([table_name,cname,ctype,fields])
                    tables.append([table_name,field_spec])
                except ParseException as x:
                    print "  No Match CREATE TABLE: {0}".format(str(x)), stmt
            if stmt[1] == 'SEQUENCE' :
                try:
                    result = seqspec.parseString(stmt[2][0])
                    sequences.append([result[0]])
                except ParseException as x:
                    print "  No Match CREATE SEQUENCE: {0}".format(str(x)), stmt

            
def main():
    with open(sys.argv[1]) as my_file:
        parse_schema(my_file)
    schemaExtract={'tables':tables,'indices':indices,'pkeys':pkeys,'constraints':constraints,'sequences':sequences,'fkeys':fkeys}
    print "Tables: ", len(schemaExtract['tables'])
    print "Sequences: ", len(schemaExtract['sequences'])
    print "PrimaryKeys: ", len(schemaExtract['pkeys'])
    print "Constraints: ", len(schemaExtract['constraints'])
    print "Indices: ", len(schemaExtract['indices'])
    print "Fkeys: ", len(schemaExtract['fkeys'])
    print 'Schema extract JSON file:',sys.argv[2]
    with open(sys.argv[2], 'w') as out_data:
        out_data.write(json.dumps(schemaExtract,indent=2,sort_keys=True ))

if __name__ == '__main__':
    main()
