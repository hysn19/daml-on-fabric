#include <eosio/eosio.hpp>
#include <eosio/crypto.hpp>
#include <vector>

using namespace eosio;
using namespace std;

class [[eosio::contract]] hello : public contract {
  public:
      using contract::contract;

      [[eosio::action]]
      void hi( name user ) {
         print( "Hello, ", user );
      }

      [[eosio::action]]
      void rawbatchwrite( std::vector<string> rawArgs ) {
        
        for (int i=0; i<rawArgs.size()-1; i++) {
            auto _prefixState = "DState:";    
            auto key = _prefixState + rawArgs[i];
            i++;
            auto value = rawArgs[i];

            checksum256 hash = get_checksum(key);

            daml_table damlobj(get_self(), get_self().value);
            auto byhash = damlobj.template get_index<"byhash"_n>();
            auto damlitr = byhash.find(hash);
            
            if (byhash.end() != damlitr) {
                byhash.modify(damlitr, same_payer, [&](auto &s) {
                    s.value = value;
                });
            } else {
                damlobj.emplace(get_self(), [&](auto &s) {
                    s.idx = damlobj.available_primary_key();
                    s.key = key;
                    s.hash = hash;
                    s.value = value;
                });
            }
        }
      }

      [[eosio::action]]
      void rawwrite( std::vector<string> rawArgs ) {

        auto _prefixState = "DState:";
        auto key = _prefixState + rawArgs[0];
        auto value = rawArgs[1];

        checksum256 hash = get_checksum(key);

        daml_table damlobj(get_self(), get_self().value);
        auto byhash = damlobj.template get_index<"byhash"_n>();
        auto damlitr = byhash.find(hash);
        
        if (byhash.end() != damlitr) {
            byhash.modify(damlitr, same_payer, [&](auto &s) {
                s.value = value;
            });
        } else {
            damlobj.emplace(get_self(), [&](auto &s) {
                s.idx = damlobj.available_primary_key();
                s.key = key;
                s.hash = hash;
                s.value = value;
            });
        }
      }

      [[eosio::action]]
      void packagewrite( std::vector<string> rawArgs ) {

        for (int i=0; i<rawArgs.size()-1; i++) {
            auto _prefixPackages = "DPackages:";
            auto key = _prefixPackages + rawArgs[i];
            i++;
            auto value = rawArgs[i];

            checksum256 hash = get_checksum(key);

            daml_table damlobj(get_self(), get_self().value);
            auto byhash = damlobj.template get_index<"byhash"_n>();
            auto damlitr = byhash.find(hash);
            
            if (byhash.end() != damlitr) {
                byhash.modify(damlitr, same_payer, [&](auto &s) {
                    s.value = value;
                });
            } else {
                damlobj.emplace(get_self(), [&](auto &s) {
                    s.idx = damlobj.available_primary_key();
                    s.key = key;
                    s.hash = hash;
                    s.value = value;
                });
            }
        }
      }

      [[eosio::action]]
      void recordtwrite( std::vector<string> rawArgs ) {

        for (int i=0; i<rawArgs.size(); i++) {
            auto _recordTime = "DRecordTime";
            auto key = _recordTime;
            auto value = rawArgs[i];

            checksum256 hash = get_checksum(key);

            daml_table damlobj(get_self(), get_self().value);
            auto byhash = damlobj.template get_index<"byhash"_n>();
            auto damlitr = byhash.find(hash);
            
            if (byhash.end() != damlitr) {
                byhash.modify(damlitr, same_payer, [&](auto &s) {
                    s.value = value;
                });
            } else {
                damlobj.emplace(get_self(), [&](auto &s) {
                    s.idx = damlobj.available_primary_key();
                    s.key = key;
                    s.hash = hash;
                    s.value = value;
                });
            }
        }
      }

      [[eosio::action]]
      void ledgeridwrite( std::vector<string> rawArgs ) {
        
        for (int i=0; i<rawArgs.size(); i++) {
            auto _ledgerID = "DLedgerID";
            auto key = _ledgerID;
            auto value = rawArgs[i];

            checksum256 hash = get_checksum(key);

            daml_table damlobj(get_self(), get_self().value);
            auto byhash = damlobj.template get_index<"byhash"_n>();
            auto damlitr = byhash.find(hash);
                
            if (byhash.end() != damlitr) {
                byhash.modify(damlitr, same_payer, [&](auto &s) {
                    s.value = value;
                });
            } else {
                damlobj.emplace(get_self(), [&](auto &s) {
                    s.idx = damlobj.available_primary_key();
                    s.key = key;
                    s.hash = hash;
                    s.value = value;
                });
            }
        }
      }

  private:

      checksum256 get_checksum(const string &data) {
          checksum256 hash = sha256(data.c_str(), data.size());
          return hash;
      }

      struct [[eosio::table]] daml {
        uint64_t idx;
        string key;
        checksum256 hash;
        string value;
        
        uint64_t primary_key() const { return idx; }
        checksum256 get_hash() const { return hash; }
      };
      
      typedef multi_index<"daml"_n, daml, 
        indexed_by<"byhash"_n, const_mem_fun<daml, checksum256, &daml::get_hash>>> daml_table;
};
